#!/usr/bin/env python3
"""
[LEGACY NOTE: Early v2.0 FIFO PoC script (superseded by `workflow_runner.py` in v4.4.0-api-ready).]

verify_v2_demo.py - Verifies Gateway v2.0 + pure Gemini.app web-tier models over POSIX FIFO IPC:
  1. Claude Code `claude_gateway_settings.json` (3 web-tier `/model` picker entries)
  2. Compiled `bin/dsh-mcp-stdio` v1.2.0 handshake & 7 MCP tools
  3. Sticky Delta incremental sync (`cold_start` -> `sticky_delta` with `cache_read_input_tokens`)
  4. >240,000-Char automatic tool output pagination (`DSH_AUTO_PAGINATED` + `dsh_read_tool_page`)
  5. 256,000-Token Soft-Watermark proactive rollover (`rollover_256k`) & live `/model` switch
"""

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import gateway_poc as gw_mod

MCP_BIN = os.path.join(BASE_DIR, "bin", "dsh-mcp-stdio")
IPC_DIR = f"/tmp/dsh_v2_verify_{os.getpid()}"


class McpStdioProc:
    def __init__(self, ipc_dir: str):
        env = os.environ.copy()
        env["DSH_IPC_DIR"] = ipc_dir
        self.proc = subprocess.Popen(
            [MCP_BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=env
        )
        self._lock = threading.Lock()
        self._nid = 0
        self._boxes = {}
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        for ln in self.proc.stdout:
            ln = ln.strip()
            if not ln:
                continue
            msg = json.loads(ln)
            with self._lock:
                box = self._boxes.get(msg.get("id"))
            if box:
                box["resp"] = msg
                box["ev"].set()

    def call(self, method: str, params: dict = None, timeout: float = 15.0) -> dict:
        with self._lock:
            self._nid += 1
            rid = self._nid
            box = {"ev": threading.Event(), "resp": None}
            self._boxes[rid] = box
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}}, ensure_ascii=False) + "\n")
        self.proc.stdin.flush()
        assert box["ev"].wait(timeout=timeout), f"Timeout on {method}"
        with self._lock:
            self._boxes.pop(rid, None)
        return box["resp"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=1.0)
        except Exception:
            self.proc.kill()


def main():
    settings_path = os.path.join(BASE_DIR, "claude_gateway_settings.json")
    with open(settings_path, "r", encoding="utf-8") as fp:
        cfg = json.load(fp)
    opts = cfg["modelPicker"]["options"]
    assert cfg["modelPicker"]["replaceBuiltInOptions"] is True and len(opts) == 3
    cat = gw_mod.build_models_catalog_response()
    assert cat["has_more"] is False and len(cat["data"]) == 3
    for row in opts:
        mid, _ = gw_mod.resolve_model_and_tier(row["model"], {})
        assert mid in gw_mod.MODEL_MAP, f"Unmapped picker option: {row['model']}"
    print(f"[PASS] 1. Claude Code `/model` picker verified ({len(opts)} Gemini.app web tiers: {[o['model'] for o in opts]})")

    gw_mod.IPC_DIR = IPC_DIR
    gw_mod.UPSTREAM_MODE = "mcp_task"
    gw_mod.SOFT_WATERMARK_TOKENS = 2000
    t_bus = threading.Thread(target=gw_mod.serve_fifo_bus, args=(IPC_DIR,), daemon=True)
    t_bus.start()
    for _ in range(40):
        if os.path.exists(os.path.join(IPC_DIR, "wake.fifo")):
            break
        time.sleep(0.05)

    mcp = McpStdioProc(IPC_DIR)
    try:
        tools = [t["name"] for t in mcp.call("tools/list")["result"]["tools"]]
        assert tools == [
            "dsh_wait_request", "dsh_await_request", "dsh_call_tool",
            "dsh_await_result", "dsh_read_tool_page", "dsh_stream_chunk", "dsh_reply",
        ], f"Unexpected tools: {tools}"
        print("[PASS] 2. Compiled `bin/dsh-mcp-stdio` v1.2.0 handshake & 7 MCP tools verified")

        stream_events = []

        def persistent_gemini_worker():
            w1 = mcp.call("tools/call", {"name": "dsh_wait_request", "arguments": {"worker_id": "w_carbon", "worker_model": "gemini-app-carbon"}})
            p1 = w1["result"]["content"][0]["text"]
            tok1 = re.search(r"\[DSH_SESSION_TOKEN:\s*(tok_[a-f0-9]+)\]", p1).group(1)
            mcp.call("tools/call", {"name": "dsh_stream_chunk", "arguments": {"session_token": tok1, "thinking": "Step1-Think", "delta": "Hello "}})
            w2 = mcp.call("tools/call", {"name": "dsh_reply", "arguments": {"session_token": tok1, "content": "World!", "wait_next": True}})

            p2 = w2["result"]["content"][0]["text"]
            assert "STICKY_DELTA Turn" in p2 and "First prompt" not in p2
            tr = mcp.call("tools/call", {"name": "dsh_call_tool", "arguments": {"session_token": tok1, "name": "Read", "arguments": {"file_path": "huge.txt"}}})
            txt_p1 = tr["result"]["content"][0]["text"]
            assert "DSH_AUTO_PAGINATED page=1/2" in txt_p1 and len(txt_p1) < 245000
            ptok = re.search(r"page_token=(pg_[a-f0-9]+)", txt_p1).group(1)
            tr_p2 = mcp.call("tools/call", {"name": "dsh_read_tool_page", "arguments": {"session_token": tok1, "page_token": ptok, "page": 2}})
            assert "EXACT_TAIL_999" in tr_p2["result"]["content"][0]["text"]

            w3 = mcp.call("tools/call", {"name": "dsh_reply", "arguments": {"session_token": tok1, "content": "Read 290KB cleanly!", "wait_next": True}})
            assert "soft watermark" in w3["result"]["content"][0]["text"]

            w4 = mcp.call("tools/call", {"name": "dsh_wait_request", "arguments": {"worker_id": "w_fast", "worker_model": "gemini-app-fast"}})
            p4 = w4["result"]["content"][0]["text"]
            tok4 = re.search(r"\[DSH_SESSION_TOKEN:\s*(tok_[a-f0-9]+)\]", p4).group(1)
            mcp.call("tools/call", {"name": "dsh_reply", "arguments": {"session_token": tok4, "content": "Switched to Fast after rollover!", "wait_next": False}})

        th = threading.Thread(target=persistent_gemini_worker, daemon=True)
        th.start()
        time.sleep(0.08)

        tdefs = [{"name": "Read", "description": "Read file", "input_schema": {"type": "object"}}]
        hdr = {"X-Session-Id": "cc_live_1", "X-Test-Harness": "1"}
        msgs = [{"role": "user", "content": "First prompt"}]

        out1 = gw_mod.execute_turn(hdr, "gemini-app-carbon", "high", "Sys", msgs, tdefs, False, on_delta=stream_events.append)
        assert out1["turn_mode"] == "cold_start" and out1["text"] == "Hello World!"
        print(f"[PASS] 3. Turn 1 (`{out1['turn_mode']}`) incremental `dsh_stream_chunk` verified -> {len(stream_events)} live SSE chunks")

        msgs.append({"role": "assistant", "content": out1["text"]})
        msgs.append({"role": "user", "content": "Now read huge.txt"})
        out2_tc = gw_mod.execute_turn(hdr, "gemini-app-carbon", "high", "Sys", msgs, tdefs, False)
        assert out2_tc["turn_mode"] == "sticky_delta" and out2_tc["cached_tokens"] > 0 and out2_tc["stop_reason"] == "tool_use"
        tc_id = out2_tc["tool_calls"][0]["id"]

        msgs.append({"role": "assistant", "content": [{"type": "tool_use", **out2_tc["tool_calls"][0]}]})
        msgs.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": tc_id, "content": ("X" * 290000) + "EXACT_TAIL_999"}]})
        out2_fin = gw_mod.execute_turn(hdr, "gemini-app-carbon", "high", "Sys", msgs, tdefs, False)
        assert out2_fin["turn_mode"] == "parked_resume" and "Read 290KB cleanly!" in out2_fin["text"]
        print(f"[PASS] 4. Turn 2 (`sticky_delta` -> `parked_resume`) & >240KB `dsh_read_tool_page` verified")

        msgs.append({"role": "assistant", "content": out2_fin["text"]})
        msgs.append({"role": "user", "content": "Continue on Fast"})
        out3 = gw_mod.execute_turn(hdr, "gemini-app-fast", "low", "Sys", msgs, tdefs, False)
        assert out3["turn_mode"] == "rollover_256k" and "Fast" in out3["text"]
        th.join(timeout=5.0)
        print(f"[PASS] 5. Turn 3 (`{out3['turn_mode']}`) 256K auto-rollover & live `/model` switch verified!")

    finally:
        mcp.close()
        shutil.rmtree(IPC_DIR, ignore_errors=True)

    print("=================================================================")
    print(" ALL 5 GATEWAY v2.0 + PURE WEB-TIER PICKER VERIFICATIONS PASSED!")
    print("=================================================================")


if __name__ == "__main__":
    main()
