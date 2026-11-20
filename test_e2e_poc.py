#!/usr/bin/env python3
"""
[LEGACY NOTE: Early v1/v2 PoC test script targeting deprecated 18895 defaults and Plane-2 mocks (superseded by `workflow_runner.py` in v4.4.0-api-ready).]

test_e2e_poc.py - End-to-End Multi-Process Verification Suite for
Gemini.app (Task + Carbon + Local MCP) -> DSH API Gateway PoC.

Spawns the real gateway process and the compiled Mach-O `bin/dsh-mcp-stdio` bridge
process inside the local macOS environment, verifying:
  1. Native Mach-O binary launch + Gemini.app Local MCP stdio handshake (2024-11-05)
  2. Anthropic /v1/messages SSE streaming + parallel tool batching + multi-hop tool loop
     + zero-UI-touch in-flight resume + lossless dsh_reply markdown/code delivery
  3. Long-running host tool keepalive relay via dsh_await_result (anti-timeout guard)
  4. 3-way concurrent sessions (OpenAI /v1/chat/completions) sharing ONE Local MCP stdio
     process with 0% session cross-talk
"""

import json
import os
import re
import shutil
import subprocess
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MCP_BIN = os.path.join(BASE_DIR, "bin", "dsh-mcp-stdio")
GATEWAY_PY = os.path.join(BASE_DIR, "gateway_poc.py")
TEST_IPC_DIR = f"/tmp/dsh_poc_ipc_{os.getpid()}"

import sys
sys.path.insert(0, BASE_DIR)
from dsh_mcp_stdio import fifo_rpc_request


def gw_request(ipc_dir: str, method: str, path: str, body_obj=None, headers=None):
    resp = fifo_rpc_request(ipc_dir, method, path, body_obj, headers=headers)
    return resp["status"], resp["body"]


class McpStdioClient:
    """Drives `bin/dsh-mcp-stdio` over real OS pipes identically to Gemini.app's McpServerRegistry."""

    def __init__(self, ipc_dir: str):
        env = os.environ.copy()
        env["DSH_IPC_DIR"] = ipc_dir
        self.proc = subprocess.Popen(
            [MCP_BIN],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        self._id_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._next_id = 0
        self._pending = {}
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            msg = json.loads(line)
            mid = msg.get("id")
            with self._id_lock:
                ev_box = self._pending.get(mid)
            if ev_box is not None:
                ev_box["resp"] = msg
                ev_box["event"].set()

    def rpc(self, method: str, params: dict = None, timeout: float = 15.0) -> dict:
        with self._id_lock:
            self._next_id += 1
            req_id = self._next_id
            box = {"event": threading.Event(), "resp": None}
            self._pending[req_id] = box
        wire = json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}, ensure_ascii=False) + "\n"
        with self._write_lock:
            self.proc.stdin.write(wire)
            self.proc.stdin.flush()
        if not box["event"].wait(timeout=timeout):
            raise TimeoutError(f"MCP stdio RPC {method} (id={req_id}) timed out")
        with self._id_lock:
            self._pending.pop(req_id, None)
        return box["resp"]

    def notify(self, method: str, params: dict = None) -> None:
        wire = json.dumps({"jsonrpc": "2.0", "method": method, "params": params or {}}, ensure_ascii=False) + "\n"
        with self._write_lock:
            self.proc.stdin.write(wire)
            self.proc.stdin.flush()

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=2.0)
        except Exception:
            self.proc.kill()


def parse_anthropic_sse(raw_sse: str) -> dict:
    blocks = {}
    stop_reason = None
    for chunk in raw_sse.strip().split("\n\n"):
        lines = chunk.strip().splitlines()
        data_line = next((l[6:] for l in lines if l.startswith("data: ")), None)
        if not data_line:
            continue
        ev = json.loads(data_line)
        t = ev.get("type")
        if t == "content_block_start":
            idx = ev["index"]
            cb = ev["content_block"]
            blocks[idx] = {"type": cb["type"], "text": "", "thinking": "", "json": "", "id": cb.get("id"), "name": cb.get("name")}
        elif t == "content_block_delta":
            idx = ev["index"]
            d = ev["delta"]
            dt = d["type"]
            if dt == "text_delta":
                blocks[idx]["text"] += d["text"]
            elif dt == "thinking_delta":
                blocks[idx]["thinking"] += d["thinking"]
            elif dt == "input_json_delta":
                blocks[idx]["json"] += d["partial_json"]
        elif t == "message_delta":
            stop_reason = ev["delta"].get("stop_reason")

    ordered = [blocks[k] for k in sorted(blocks.keys())]
    tool_uses = [
        {"id": b["id"], "name": b["name"], "input": json.loads(b["json"] or "{}")}
        for b in ordered
        if b["type"] == "tool_use"
    ]
    texts = "".join(b["text"] for b in ordered if b["type"] == "text")
    thinkings = "".join(b["thinking"] for b in ordered if b["type"] == "thinking")
    return {"stop_reason": stop_reason, "tool_uses": tool_uses, "text": texts, "thinking": thinkings}


def run_suite():
    print("=================================================================")
    print(" Gemini.app Local MCP + API Gateway PoC - End-to-End Verification")
    print("=================================================================")

    env = os.environ.copy()
    env["DSH_IPC_DIR"] = TEST_IPC_DIR
    env["DSH_FIFO_ONLY"] = "1"
    env["DSH_BATCH_WINDOW_SEC"] = "0.18"
    env["DSH_RELAY_SEC"] = "0.55"
    env["DSH_QUIET"] = "1"

    gw_proc = subprocess.Popen(
        ["/usr/bin/python3", "-u", GATEWAY_PY],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        wake_path = os.path.join(TEST_IPC_DIR, "wake.fifo")
        for _ in range(40):
            if os.path.exists(wake_path):
                break
            time.sleep(0.05)
        assert os.path.exists(wake_path), f"Gateway FIFO bus did not start! stderr={gw_proc.stderr.read()}"
        st, raw = gw_request(TEST_IPC_DIR, "GET", "/health")
        assert st == 200 and json.loads(raw)["ok"] is True
        print("[PASS] 0. Gateway daemon booted cleanly inside macOS sandbox:", TEST_IPC_DIR)

        mcp = McpStdioClient(TEST_IPC_DIR)
        try:
            # --- TEST 1: Handshake & Tools Discovery ---
            t0 = time.time()
            init_res = mcp.rpc("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "com.google.GeminiMacOS", "version": "1.144.4.1467"},
            })
            mcp.notify("notifications/initialized")
            list_res = mcp.rpc("tools/list", {})
            dt_ms = (time.time() - t0) * 1000
            assert init_res["result"]["protocolVersion"] == "2024-11-05"
            tool_names = [t["name"] for t in list_res["result"]["tools"]]
            assert tool_names == ["dsh_call_tool", "dsh_await_result", "dsh_reply"], f"Unexpected tools: {tool_names}"
            print(f"[PASS] 1. Compiled Mach-O `dsh-mcp-stdio` handshake & tools/list verified in {dt_ms:.1f}ms -> {tool_names}")

            # --- TEST 2: Parallel Tool Batching + Multi-Hop In-Flight Loop + Lossless dsh_reply ---
            expected_md = "### 审阅结论\n```go\nfunc ServeMCP() { /* 零损耗保留缩进与符号 <ok> & \"quote\" */ }\n```\n全部 2 处文件核对完毕！"
            expected_thk = "先并行读取 main.go 和 config.go，再执行 uname 校验架构，最后通过 dsh_reply 原样回传。"

            def simulated_gemini_task_worker():
                _, p_raw = gw_request(TEST_IPC_DIR, "GET", "/pending-prompts?wait=5")
                p_obj = json.loads(p_raw)
                assert p_obj["has_prompt"], "Worker expected initial prompt!"
                prompt_txt = p_obj["item"]["prompt"]
                m = re.search(r"\[DSH_SESSION_TOKEN:\s*(tok_[a-f0-9]+)\]", prompt_txt)
                assert m, f"Missing token in prompt: {prompt_txt[:120]}"
                tok = m.group(1)

                res_parallel = [None, None]

                def call_read(idx, path_arg):
                    res_parallel[idx] = mcp.rpc("tools/call", {
                        "name": "dsh_call_tool",
                        "arguments": {"session_token": tok, "name": "Read", "arguments": {"file_path": path_arg}},
                    })

                th1 = threading.Thread(target=call_read, args=(0, "go-gateway/main.go"))
                th2 = threading.Thread(target=call_read, args=(1, "go-gateway/config.go"))
                th1.start()
                time.sleep(0.02)
                th2.start()
                th1.join()
                th2.join()

                txt0 = res_parallel[0]["result"]["content"][0]["text"]
                txt1 = res_parallel[1]["result"]["content"][0]["text"]
                assert "package main // main.go" in txt0, f"Unexpected tool result 0: {txt0}"
                assert "MaxLiveWorkers = 8" in txt1, f"Unexpected tool result 1: {txt1}"

                r_bash = mcp.rpc("tools/call", {
                    "name": "dsh_call_tool",
                    "arguments": {"session_token": tok, "name": "Bash", "arguments": {"command": "uname -m"}},
                })
                assert "arm64" in r_bash["result"]["content"][0]["text"]

                r_fin = mcp.rpc("tools/call", {
                    "name": "dsh_reply",
                    "arguments": {"session_token": tok, "thinking": expected_thk, "content": expected_md},
                })
                assert r_fin["result"]["isError"] is False

            worker_t = threading.Thread(target=simulated_gemini_task_worker, daemon=True)
            worker_t.start()

            tools_def = [
                {"name": "Read", "description": "Read file", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
                {"name": "Bash", "description": "Run command", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}},
            ]
            msgs = [{"role": "user", "content": "请帮我读取 main.go 和 config.go 并检查本机架构"}]

            t_turn1 = time.time()
            st1, sse1 = gw_request(TEST_IPC_DIR, "POST", "/v1/messages", {
                "model": "gemini-4-carbon", "stream": True, "tools": tools_def, "messages": msgs,
            }, headers={"X-Session-Id": "claude_code_sess_1"})
            assert st1 == 200
            p1 = parse_anthropic_sse(sse1)
            assert p1["stop_reason"] == "tool_use"
            assert len(p1["tool_uses"]) == 2, f"Expected 2 batched tool calls, got: {p1['tool_uses']}"
            t_turn1_ms = (time.time() - t_turn1) * 1000

            msgs.append({"role": "assistant", "content": [{"type": "tool_use", **u} for u in p1["tool_uses"]]})
            msgs.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": p1["tool_uses"][0]["id"], "content": "package main // main.go"},
                {"type": "tool_result", "tool_use_id": p1["tool_uses"][1]["id"], "content": "const MaxLiveWorkers = 8"},
            ]})
            t_resume = time.time()
            st2, sse2 = gw_request(TEST_IPC_DIR, "POST", "/v1/messages", {
                "model": "gemini-4-carbon", "stream": True, "tools": tools_def, "messages": msgs,
            }, headers={"X-Session-Id": "claude_code_sess_1"})
            p2 = parse_anthropic_sse(sse2)
            t_resume_ms = (time.time() - t_resume) * 1000
            assert p2["stop_reason"] == "tool_use" and len(p2["tool_uses"]) == 1
            assert p2["tool_uses"][0]["name"] == "Bash"

            _, q_check = gw_request(TEST_IPC_DIR, "GET", "/pending-prompts?wait=0.01")
            assert json.loads(q_check)["has_prompt"] is False, "Turn 2 should NOT enqueue a UI prompt!"

            msgs.append({"role": "assistant", "content": [{"type": "tool_use", **p2["tool_uses"][0]}]})
            msgs.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": p2["tool_uses"][0]["id"], "content": "arm64\n"},
            ]})
            st3, sse3 = gw_request(TEST_IPC_DIR, "POST", "/v1/messages", {
                "model": "gemini-4-carbon", "stream": True, "tools": tools_def, "messages": msgs,
            }, headers={"X-Session-Id": "claude_code_sess_1"})
            p3 = parse_anthropic_sse(sse3)
            worker_t.join(timeout=5.0)
            assert p3["stop_reason"] == "end_turn"
            assert p3["thinking"] == expected_thk
            assert p3["text"] == expected_md
            print(f"[PASS] 2. Anthropic SSE 3-turn loop passed! (2 parallel tools batched in {t_turn1_ms:.0f}ms, in-flight stdio resume in {t_resume_ms:.0f}ms, 0 UI touches on Turns 2-3, 100% lossless Markdown/code)")

            # --- TEST 3: Long-Running Tool Keepalive Relay (dsh_await_result) ---
            relay_hops_observed = []

            def relay_worker():
                _, p_raw = gw_request(TEST_IPC_DIR, "GET", "/pending-prompts?wait=5")
                tok = json.loads(p_raw)["item"]["session_token"]
                r = mcp.rpc("tools/call", {
                    "name": "dsh_call_tool",
                    "arguments": {"session_token": tok, "name": "Bash", "arguments": {"command": "sleep 1.2 && echo BUILD_OK"}},
                })
                while "Immediately call dsh_await_result" in r["result"]["content"][0]["text"]:
                    txt = r["result"]["content"][0]["text"]
                    m_cid = re.search(r'"call_id":"(call_[a-f0-9]+)"', txt)
                    assert m_cid, f"Missing call_id in relay text: {txt}"
                    relay_hops_observed.append(m_cid.group(1))
                    r = mcp.rpc("tools/call", {
                        "name": "dsh_await_result",
                        "arguments": {"session_token": tok, "call_id": m_cid.group(1)},
                    })
                assert "BUILD_OK" in r["result"]["content"][0]["text"]
                mcp.rpc("tools/call", {
                    "name": "dsh_reply",
                    "arguments": {"session_token": tok, "content": "Long build succeeded after relay!"},
                })

            rt = threading.Thread(target=relay_worker, daemon=True)
            rt.start()
            _, r_json = gw_request(TEST_IPC_DIR, "POST", "/v1/messages", {
                "model": "gemini-4-carbon", "stream": False, "tools": tools_def,
                "messages": [{"role": "user", "content": "Run slow build"}],
            }, headers={"X-Session-Id": "slow_build_sess"})
            r1_obj = json.loads(r_json)
            tc_id = r1_obj["content"][0]["id"]
            time.sleep(1.25)
            _, r2_json = gw_request(TEST_IPC_DIR, "POST", "/v1/messages", {
                "model": "gemini-4-carbon", "stream": False, "tools": tools_def,
                "messages": [
                    {"role": "user", "content": "Run slow build"},
                    {"role": "assistant", "content": r1_obj["content"]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tc_id, "content": "BUILD_OK"}]},
                ],
            }, headers={"X-Session-Id": "slow_build_sess"})
            rt.join(timeout=5.0)
            r2_obj = json.loads(r2_json)
            assert len(relay_hops_observed) >= 2, f"Expected >=2 relay hops, got {len(relay_hops_observed)}"
            assert r2_obj["content"][0]["text"] == "Long build succeeded after relay!"
            print(f"[PASS] 3. Long-running tool keepalive relay (`dsh_await_result`) survived {len(relay_hops_observed)} timeout windows without dropping!")

            # --- TEST 4: 3-Way Concurrent Multi-Window Isolation on Shared Single MCP Pipe ---
            def multi_dispatcher():
                sub_threads = []
                for _ in range(3):
                    _, p_raw = gw_request(TEST_IPC_DIR, "GET", "/pending-prompts?wait=5")
                    item = json.loads(p_raw)["item"]
                    tok = item["session_token"]
                    prompt_txt = item["prompt"]
                    tag = re.search(r"TAG=(TENANT_[A-Z])", prompt_txt).group(1)

                    def handle_one(t_tok, t_tag):
                        tr = mcp.rpc("tools/call", {
                            "name": "dsh_call_tool",
                            "arguments": {"session_token": t_tok, "name": "EchoTag", "arguments": {"who": t_tag}},
                        })
                        val = tr["result"]["content"][0]["text"]
                        mcp.rpc("tools/call", {
                            "name": "dsh_reply",
                            "arguments": {"session_token": t_tok, "content": f"VERIFIED:{t_tag}->{val}"},
                        })

                    st_t = threading.Thread(target=handle_one, args=(tok, tag), daemon=True)
                    st_t.start()
                    sub_threads.append(st_t)
                for st_t in sub_threads:
                    st_t.join(timeout=8.0)

            disp_t = threading.Thread(target=multi_dispatcher, daemon=True)
            disp_t.start()

            client_results = {}
            oai_tools = [{"type": "function", "function": {"name": "EchoTag", "description": "Echo", "parameters": {"type": "object", "properties": {"who": {"type": "string"}}}}}]

            def run_oai_client(tag_name):
                hdr = {"X-Session-Id": f"conc_{tag_name}"}
                m_list = [{"role": "user", "content": f"Please process TAG={tag_name}"}]
                _, raw1 = gw_request(TEST_IPC_DIR, "POST", "/v1/chat/completions", {
                    "model": "gemini-4-carbon", "stream": False, "tools": oai_tools, "messages": m_list,
                }, headers=hdr)
                o1 = json.loads(raw1)
                tc = o1["choices"][0]["message"]["tool_calls"][0]
                arg_who = json.loads(tc["function"]["arguments"])["who"]
                assert arg_who == tag_name, f"Cross-talk detected! {tag_name} got tool arg {arg_who}"
                m_list.append(o1["choices"][0]["message"])
                m_list.append({"role": "tool", "tool_call_id": tc["id"], "content": f"HOST_ACK_{tag_name}"})
                _, raw2 = gw_request(TEST_IPC_DIR, "POST", "/v1/chat/completions", {
                    "model": "gemini-4-carbon", "stream": False, "tools": oai_tools, "messages": m_list,
                }, headers=hdr)
                o2 = json.loads(raw2)
                client_results[tag_name] = o2["choices"][0]["message"]["content"]

            c_threads = [threading.Thread(target=run_oai_client, args=(t,)) for t in ("TENANT_A", "TENANT_B", "TENANT_C")]
            for ct in c_threads:
                ct.start()
            for ct in c_threads:
                ct.join(timeout=10.0)
            disp_t.join(timeout=5.0)

            for t in ("TENANT_A", "TENANT_B", "TENANT_C"):
                expected = f"VERIFIED:{t}->HOST_ACK_{t}"
                assert client_results.get(t) == expected, f"Mismatch for {t}: {client_results.get(t)}"
            print("[PASS] 4. 3-way concurrent OpenAI `/v1/chat/completions` sessions over 1 shared Local MCP stdio process: 0% cross-talk!")
            print("          Results:", client_results)

        finally:
            mcp.close()
    finally:
        gw_proc.terminate()
        try:
            gw_proc.wait(timeout=2.0)
        except Exception:
            gw_proc.kill()
        shutil.rmtree(TEST_IPC_DIR, ignore_errors=True)

    print("=================================================================")
    print(" ALL 4 END-TO-END TESTS PASSED ON LOCAL MACOS!")
    print("=================================================================")


if __name__ == "__main__":
    run_suite()
