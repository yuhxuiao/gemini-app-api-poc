#!/usr/bin/env python3
"""
[LEGACY NOTE: Early v3 prototype protocol test targeting deprecated 6-model catalog (superseded by `workflow_runner.py` in v4.4.0-api-ready).]

deep_protocol_and_stream_test.py - Deep Functional & Streaming/TTFT Verification Suite:
  1. `/v1/models`, `/v1/messages/count_tokens`, and all 3 Claude Code Fast-Path Probes
  2. All 6 Models + Thinking Tiers + Signature Verification (`signature_delta`)
  3. OpenAI `/v1/responses` (`function_call` -> `function_call_output`) & `/v1/chat/completions`
  4. True SSE Streaming TTFT vs Buffered MCP `dsh_reply` TTFT & Chunk Cadence Comparison
"""

import http.client
import json
import os
import shutil
import subprocess
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MCP_BIN = os.path.join(BASE_DIR, "bin", "dsh-mcp-stdio")
GATEWAY_PY = os.path.join(BASE_DIR, "gateway_poc.py")
PORT = int(os.environ.get("TEST_PORT", "18896"))
IPC_DIR = f"/tmp/dsh_deep_ipc_{os.getpid()}"


class McpStdioWorker:
    def __init__(self, ipc_dir: str, port: int):
        env = os.environ.copy()
        env["DSH_IPC_DIR"] = ipc_dir
        env["DSH_GATEWAY_PORT"] = str(port)
        self.proc = subprocess.Popen(
            [MCP_BIN],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, env=env,
        )
        self._lock = threading.Lock()
        self._wlock = threading.Lock()
        self._nid = 0
        self._pending = {}
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            msg = json.loads(line)
            mid = msg.get("id")
            with self._lock:
                box = self._pending.get(mid)
            if box:
                box["resp"] = msg
                box["ev"].set()

    def rpc(self, method: str, params: dict = None, timeout: float = 25.0) -> dict:
        with self._lock:
            self._nid += 1
            rid = self._nid
            box = {"ev": threading.Event(), "resp": None}
            self._pending[rid] = box
        wire = json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}}, ensure_ascii=False) + "\n"
        with self._wlock:
            self.proc.stdin.write(wire)
            self.proc.stdin.flush()
        if not box["ev"].wait(timeout=timeout):
            raise TimeoutError(f"MCP RPC {method} timed out")
        with self._lock:
            self._pending.pop(rid, None)
        return box["resp"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=2.0)
        except Exception:
            self.proc.kill()


def http_json(port: int, method: str, path: str, body: dict = None, headers: dict = None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30.0)
    hdrs = {"Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    payload = None
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    conn.request(method, path, body=payload, headers=hdrs)
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8")
    st = resp.status
    rhdrs = dict(resp.getheaders())
    conn.close()
    return st, rhdrs, json.loads(raw)


def measure_sse_stream_timing(port: int, path: str, body: dict, headers: dict = None) -> dict:
    t0 = time.perf_counter()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30.0)
    hdrs = {"Content-Type": "application/json", "Accept": "text/event-stream"}
    if headers:
        hdrs.update(headers)
    conn.request("POST", path, body=json.dumps(body, ensure_ascii=False).encode("utf-8"), headers=hdrs)
    resp = conn.getresponse()
    assert resp.status == 200, f"Expected 200 SSE, got {resp.status}"

    first_content_ms = None
    chunk_timestamps_ms = []
    text_parts = []
    think_parts = []
    signature_seen = False
    stop_reason = None

    buf = ""
    while True:
        raw = resp.fp.read1(4096) if hasattr(resp.fp, "read1") else resp.fp.read(256)
        if not raw:
            break
        now_ms = (time.perf_counter() - t0) * 1000.0
        buf += raw.decode("utf-8", errors="replace")
        while "\n\n" in buf:
            frame, buf = buf.split("\n\n", 1)
            data_line = next((l[6:] for l in frame.splitlines() if l.startswith("data: ")), None)
            if not data_line or data_line == "[DONE]":
                continue
            ev = json.loads(data_line)
            if ev.get("type") == "content_block_delta":
                d = ev["delta"]
                dt = d.get("type")
                if dt in ("text_delta", "thinking_delta", "input_json_delta"):
                    if first_content_ms is None:
                        first_content_ms = now_ms
                    chunk_timestamps_ms.append(round(now_ms, 1))
                if dt == "text_delta":
                    text_parts.append(d["text"])
                elif dt == "thinking_delta":
                    think_parts.append(d["thinking"])
                elif dt == "signature_delta" and d.get("signature"):
                    signature_seen = True
            elif ev.get("type") == "message_delta":
                stop_reason = ev["delta"].get("stop_reason")

    total_ms = (time.perf_counter() - t0) * 1000.0
    conn.close()
    return {
        "ttft_ms": round(first_content_ms or total_ms, 1),
        "total_ms": round(total_ms, 1),
        "chunk_count": len(chunk_timestamps_ms),
        "chunk_times_ms": chunk_timestamps_ms,
        "text": "".join(text_parts),
        "thinking": "".join(think_parts),
        "signature_seen": signature_seen,
        "stop_reason": stop_reason,
    }


def main():
    env = os.environ.copy()
    env["DSH_IPC_DIR"] = IPC_DIR
    env["DSH_GATEWAY_HOST"] = "127.0.0.1"
    env["DSH_GATEWAY_PORT"] = str(PORT)
    env["DSH_BATCH_WINDOW_SEC"] = "0.10"
    env["DSH_RELAY_SEC"] = "15.0"
    env["DSH_QUIET"] = "1"

    gw = subprocess.Popen(
        ["/usr/bin/python3", "-u", GATEWAY_PY],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )

    try:
        ready = False
        for _ in range(40):
            try:
                st, _, obj = http_json(PORT, "GET", "/healthz")
                if st == 200 and obj.get("ok"):
                    ready = True
                    break
            except Exception:
                time.sleep(0.05)
        assert ready, f"Gateway HTTP failed to start: {gw.stderr.read()}"
        print(f"[PASS] 1. Gateway HTTP + FIFO Bus online at http://127.0.0.1:{PORT}")

        mcp = McpStdioWorker(IPC_DIR, PORT)
        try:
            init_r = mcp.rpc("initialize", {"protocolVersion": "2024-11-05"})
            tools_r = mcp.rpc("tools/list", {})
            exposed = [t["name"] for t in tools_r["result"]["tools"]]
            print(f"[PASS] 2. Local MCP binary initialized (`{init_r['result']['protocolVersion']}`): {exposed}")

            st_m, _, m_list = http_json(PORT, "GET", "/v1/models")
            assert st_m == 200 and len(m_list["data"]) >= 24
            st_ct, _, ct_obj = http_json(PORT, "POST", "/v1/messages/count_tokens", {
                "model": "claude-sonnet-4-5",
                "system": "You are an assistant.",
                "messages": [{"role": "user", "content": "Hello world"}],
                "tools": [{"name": "Read", "input_schema": {"type": "object"}}],
            })
            assert st_ct == 200 and ct_obj["input_tokens"] > 5

            t_fp = time.perf_counter()
            _, _, q_obj = http_json(PORT, "POST", "/v1/messages", {
                "model": "claude-sonnet-4-5", "max_tokens": 1,
                "messages": [{"role": "user", "content": "quota check"}],
            })
            _, _, sg_obj = http_json(PORT, "POST", "/v1/messages", {
                "model": "claude-sonnet-4-5", "max_tokens": 128,
                "messages": [{"role": "user", "content": "[SUGGESTION MODE: next command]"}],
            })
            _, _, sp_obj = http_json(PORT, "POST", "/v1/messages", {
                "model": "claude-sonnet-4-5", "max_tokens": 64,
                "messages": [{"role": "user", "content": "Context\nDescribe your most recent action in 3-5 words."}],
            })
            fp_ms = (time.perf_counter() - t_fp) * 1000.0
            assert q_obj["content"][0]["text"] == "OK"
            assert sg_obj["content"][0]["text"] == ""
            assert "Analyzing" in sp_obj["content"][0]["text"]
            _, _, pend_chk = http_json(PORT, "GET", "/pending-prompts?wait=0.01")
            assert pend_chk["has_prompt"] is False
            print(f"[PASS] 3. `/v1/models` ({len(m_list['data'])} entries), `/v1/messages/count_tokens` ({ct_obj['input_tokens']} tok), and 3 Fast-Path Probes handled in {fp_ms:.1f}ms total")

            model_matrix = [
                ("gemini-4-carbon-high", "gemini-4-carbon/high"),
                ("gemini-4-argon", "gemini-4-argon/medium"),
                ("gemini-4-hydrogen-xhigh", "gemini-4-hydrogen/xhigh"),
                ("gemini-3.8-flash-low", "gemini-3.8-flash/low"),
                ("claude-opus-5-5-high", "claude-opus-5-5/high"),
                ("claude-sonnet-4-5-20250929", "claude-sonnet-5-5/high"),
            ]

            def matrix_worker():
                for _ in range(len(model_matrix)):
                    _, _, p_obj = http_json(PORT, "GET", "/pending-prompts?wait=5")
                    it = p_obj["item"]
                    mcp.rpc("tools/call", {
                        "name": "dsh_reply",
                        "arguments": {
                            "session_token": it["session_token"],
                            "thinking": f"routed:{it['model']}/{it['tier']}",
                            "content": f"ACK:{it['model']}:{it['tier']}",
                        },
                    })

            mt = threading.Thread(target=matrix_worker, daemon=True)
            mt.start()
            routed_summary = []
            for idx, (req_m, expected_served) in enumerate(model_matrix):
                body = {
                    "model": req_m, "max_tokens": 1024, "stream": False,
                    "messages": [{"role": "user", "content": f"ping model {idx}"}],
                }
                if "20250929" in req_m:
                    body["thinking"] = {"type": "enabled", "budget_tokens": 24000}
                st, rhdrs, r_obj = http_json(PORT, "POST", "/v1/messages", body, headers={"X-Session-Id": f"m_{idx}"})
                assert st == 200
                served = rhdrs.get("X-Served-Model", "")
                assert served == expected_served, f"Model routing mismatch: {req_m} -> {served} != {expected_served}"
                assert r_obj["content"][0]["type"] == "thinking" and r_obj["content"][0].get("signature")
                routed_summary.append(f"{req_m}->{served}")
            mt.join(timeout=5.0)
            print("[PASS] 4. All 6 model families & thinking-tier mappings verified:", ", ".join(routed_summary))

            def responses_worker():
                _, _, p_obj = http_json(PORT, "GET", "/pending-prompts?wait=5")
                tok = p_obj["item"]["session_token"]
                tr = mcp.rpc("tools/call", {
                    "name": "dsh_call_tool",
                    "arguments": {"session_token": tok, "name": "GetArch", "arguments": {}},
                })
                val = tr["result"]["content"][0]["text"]
                mcp.rpc("tools/call", {
                    "name": "dsh_reply",
                    "arguments": {"session_token": tok, "content": f"RESPONSES_OK:{val}"},
                })

            threading.Thread(target=responses_worker, daemon=True).start()
            _, _, resp1 = http_json(PORT, "POST", "/v1/responses", {
                "model": "gemini-4-carbon",
                "input": "Check arch",
                "tools": [{"name": "GetArch", "description": "Get arch", "parameters": {"type": "object", "properties": {}}}],
            }, headers={"X-Session-Id": "resp_api_1"})
            fc_item = next(x for x in resp1["output"] if x["type"] == "function_call")
            _, _, resp2 = http_json(PORT, "POST", "/v1/responses", {
                "model": "gemini-4-carbon",
                "input": [
                    {"type": "message", "role": "user", "content": "Check arch"},
                    fc_item,
                    {"type": "function_call_output", "call_id": fc_item["call_id"], "output": "arm64-darwin"},
                ],
                "tools": [{"name": "GetArch", "description": "Get arch", "parameters": {"type": "object", "properties": {}}}],
            }, headers={"X-Session-Id": "resp_api_1"})
            assert resp2["output_text"] == "RESPONSES_OK:arm64-darwin"
            print("[PASS] 5. OpenAI `/v1/responses` function_call -> function_call_output loop verified!")

            # Streaming TTFT comparison
            def buffered_mcp_worker():
                _, _, p_obj = http_json(PORT, "GET", "/pending-prompts?wait=5")
                tok = p_obj["item"]["session_token"]
                time.sleep(0.42)
                mcp.rpc("tools/call", {
                    "name": "dsh_reply",
                    "arguments": {"session_token": tok, "thinking": "思考完毕", "content": "这是通过 dsh_reply 一次性提交的完整回复。"},
                })

            threading.Thread(target=buffered_mcp_worker, daemon=True).start()
            b1_metrics = measure_sse_stream_timing(PORT, "/v1/messages", {
                "model": "gemini-4-carbon", "stream": True, "max_tokens": 1024,
                "messages": [{"role": "user", "content": "测试标准 MCP dsh_reply 流式表现"}],
            }, headers={"X-Session-Id": "stream_b1"})

            def incremental_stream_worker():
                _, _, p_obj = http_json(PORT, "GET", "/pending-prompts?wait=5")
                tok = p_obj["item"]["session_token"]
                time.sleep(0.08)
                mcp.rpc("tools/call", {"name": "dsh_stream_chunk", "arguments": {"session_token": tok, "thinking": "第一步分析..."}})
                for seg in ["第一段流式输出，", "第二段流式输出，", "第三段流式输出，", "第四段流式收尾。"]:
                    time.sleep(0.09)
                    mcp.rpc("tools/call", {"name": "dsh_stream_chunk", "arguments": {"session_token": tok, "delta": seg}})
                mcp.rpc("tools/call", {"name": "dsh_reply", "arguments": {"session_token": tok, "content": ""}})

            threading.Thread(target=incremental_stream_worker, daemon=True).start()
            b2_metrics = measure_sse_stream_timing(PORT, "/v1/messages", {
                "model": "gemini-4-carbon", "stream": True, "max_tokens": 1024,
                "messages": [{"role": "user", "content": "测试增量真流式分片表现"}],
            }, headers={"X-Session-Id": "stream_b2"})

            assert b1_metrics["signature_seen"] and b2_metrics["signature_seen"]
            print("[PASS] 6. SSE Streaming TTFT Comparison Measured:")
            print(f"          - Mode A (Pure `Local MCP dsh_reply` buffered): TTFT={b1_metrics['ttft_ms']}ms, Total={b1_metrics['total_ms']}ms, Chunks={b1_metrics['chunk_count']}")
            print(f"          - Mode B (Incremental chunk stream / ACP):      TTFT={b2_metrics['ttft_ms']}ms, Total={b2_metrics['total_ms']}ms, Chunks={b2_metrics['chunk_count']}, Timestamps={b2_metrics['chunk_times_ms']}ms")

        finally:
            mcp.close()
    finally:
        gw.terminate()
        try:
            gw.wait(timeout=2.0)
        except Exception:
            gw.kill()
        shutil.rmtree(IPC_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
