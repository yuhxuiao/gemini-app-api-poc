#!/usr/bin/env python3
"""
workflow_runner.py - Automated 6-Stage Build, Unit, Torture, E2E & Smoke Pipeline for Dual-Plane v4.5
Works seamlessly both in normal macOS Terminal (TCP 127.0.0.1 + AF_UNIX sockets) AND inside
offline Seatbelt sandboxes (automatic `socket.socketpair()` + POSIX FIFO bus fallback).
"""

import hashlib
import http.client
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
BIN_DIR = os.path.join(PROJECT_DIR, "bin")
RUNTIME_DIR = os.path.join(PROJECT_DIR, "runtime")
MANIFEST_PATH = os.path.join(RUNTIME_DIR, "build_manifest.json")
REPORT_PATH = os.path.join(PROJECT_DIR, "workflow_report.json")
TARGET_VERSION = "4.6.0-clean-carbon"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fp:
        while True:
            chunk = fp.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def make_call_envelope(nonce: str, tool_name: str, tool_input: dict) -> str:
    tag = "dsh_" + "call_" + nonce
    payload = json.dumps({"name": tool_name, "input": tool_input}, ensure_ascii=False)
    return f"<{tag}>{payload}</{tag}>"


def make_raw_envelope(nonce: str, tool_name: str, kv_pairs: List[Tuple[str, str]]) -> str:
    ctag = "dsh_" + "call_" + nonce
    atag = "dsh_" + "arg_" + nonce
    inner = "".join(f'<{atag} name="{k}">{v}</{atag}>' for k, v in kv_pairs)
    return f'<{ctag} tool="{tool_name}" mode="raw">{inner}</{ctag}>'


def build_wrb_frame(
    cid: str,
    rid: str,
    rcid: str,
    thinking_snap: str,
    text_snap: str,
    step_thoughts: Optional[List[str]] = None,
    canvas_entries: Optional[List[Any]] = None,
    routed_model: str = "",
) -> str:
    cand: List[Any] = [None] * 38
    cand[0] = rcid
    cand[1] = [text_snap]
    if canvas_entries is not None:
        cand[30] = canvas_entries
    if thinking_snap:
        cand[37] = [[thinking_snap]]
    elif step_thoughts:
        cand[37] = [None, [[[s]] for s in step_thoughts]]
    part: List[Any] = [None, [cid, rid], None, None, [cand]]
    if routed_model:
        while len(part) <= 43:
            part.append(None)
        part[42] = routed_model
    outer = [["wrb.fr", None, json.dumps(part, ensure_ascii=False)]]
    payload = json.dumps(outer, ensure_ascii=False)
    return f"{len(payload)}\n{payload}\n"


class SocketPairHTTPClient:
    """Executes real HTTP/1.1 requests (including chunked/SSE responses) against `GatewayHTTPHandler` over `socket.socketpair()`."""

    def __init__(self, handler_cls: Any):
        self.handler_cls = handler_cls

    def request(self, method: str, path: str, body: Optional[dict] = None, headers: Optional[dict] = None) -> Tuple[int, dict, str]:
        c_sock, s_sock = socket.socketpair()
        srv_dummy = type("DummyServer", (), {"server_name": "localhost", "server_port": 18896})()

        def run_server_side():
            try:
                self.handler_cls(s_sock, ("127.0.0.1", 0), srv_dummy)
            except Exception:
                pass
            finally:
                try:
                    s_sock.close()
                except Exception:
                    pass

        t = threading.Thread(target=run_server_side, daemon=True)
        t.start()

        conn = http.client.HTTPConnection("127.0.0.1", 18896, timeout=30.0)
        conn.sock = c_sock
        try:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
            hdrs = {"Content-Type": "application/json"}
            if headers:
                hdrs.update(headers)
            conn.request(method, path, body=payload, headers=hdrs)
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8", errors="replace")
            resp_hdrs = {k.lower(): v for k, v in resp.getheaders()}
            return resp.status, resp_hdrs, raw
        finally:
            try:
                conn.close()
            except Exception:
                pass
            t.join(timeout=2.0)


class MockWebKitStreamPeer:
    """Simulates `dsh-webkit-bridge` v4.5 `StreamGenerate` `wrb.fr` & `direct_delta` frames over `socket.socketpair()`."""

    def __init__(self):
        self.requests_log: List[dict] = []
        self.cancelled_req_ids: List[str] = []
        self.drained_req_ids: List[str] = []
        self.deleted_cids: List[str] = []
        self._cancel_lock = threading.Lock()
        self._cancel_events: Dict[str, threading.Event] = {}
        self.warmup_fail_remaining = 0
        self.canned_err_on_cursor_once = False
        self.premature_eof_once = False
        self.carbon_timeout_once = False
        self.http_429_remaining = 0
        self.first_token_timeout_once = False
        self.err_1097_on_cursor_once = False

    def create_connected_socket(self) -> socket.socket:
        c_sock, s_sock = socket.socketpair()
        threading.Thread(target=self._handle_conn, args=(s_sock,), daemon=True).start()
        return c_sock

    def _handle_conn(self, conn: socket.socket) -> None:
        cancel_ev: Optional[threading.Event] = None
        req_id = "r1"
        try:
            buf = b""
            while b"\n" not in buf:
                c = conn.recv(16384)
                if not c:
                    return
                buf += c
            req = json.loads(buf.split(b"\n", 1)[0].decode("utf-8"))
            op = req.get("op", "status")
            req_id = str(req.get("reqId", "r1"))
            if op == "status":
                conn.sendall((json.dumps({
                    "ok": True,
                    "bridge_version": TARGET_VERSION,
                    "plane": "webkit_mock",
                    "logged_in": True,
                    "ready": True,
                    "page_loaded": True,
                    "direct_delta": True,
                    "account": "dev.user@gmail.com",
                    "slots": 2,
                }) + "\n").encode("utf-8"))
                return
            if op == "delete_conversations":
                cids = [str(c) for c in (req.get("cids") or []) if str(c).startswith("c_")]
                with self._cancel_lock:
                    for c in cids:
                        if c not in self.deleted_cids:
                            self.deleted_cids.append(c)
                conn.sendall((json.dumps({"ok": True, "deleted": cids, "requested": len(cids), "errors": []}) + "\n").encode("utf-8"))
                return
            if op == "list_conversations":
                conn.sendall((json.dumps({"ok": True, "cids": []}) + "\n").encode("utf-8"))
                return
            if op == "background_drain":
                target_id = str(req.get("targetReqId") or req.get("reqId") or "")
                with self._cancel_lock:
                    self.drained_req_ids.append(target_id)
                    ev = self._cancel_events.get(target_id)
                    if ev is not None:
                        ev.set()
                try:
                    conn.sendall((json.dumps({"ok": True, "draining": True, "targetReqId": target_id}) + "\n").encode("utf-8"))
                except OSError:
                    pass
                return
            if op in ("abort", "cancel"):
                target_id = str(req.get("targetReqId") or req.get("reqId") or "")
                with self._cancel_lock:
                    self.cancelled_req_ids.append(target_id)
                    ev = self._cancel_events.get(target_id)
                    if ev is not None:
                        ev.set()
                try:
                    conn.sendall((json.dumps({"ok": True, "aborted": True, "targetReqId": target_id}) + "\n").encode("utf-8"))
                except OSError:
                    pass
                return
            if op in ("open_login", "login", "switch_account", "auth_invalidate", "shutdown"):
                conn.sendall((json.dumps({"ok": True, "op": op, "switched": (op == "switch_account"), "login_window": "opened", "bridge_version": TARGET_VERSION}) + "\n").encode("utf-8"))
                return

            cancel_ev = threading.Event()
            with self._cancel_lock:
                self._cancel_events[req_id] = cancel_ev

            freq = json.loads(req.get("fReq", "[null,\"[]\"]"))
            inner = json.loads(freq[1])
            prompt = str(inner[0][0])
            cursor_in = inner[2][:3]
            slot_79 = inner[79]
            slot_80 = inner[80]
            self.requests_log.append({
                "prompt": prompt,
                "cursor": cursor_in,
                "turn_17": inner[17],
                "canvas_32": inner[32],
                "slot_79": slot_79,
                "slot_80": slot_80,
                "extra_headers": req.get("extraHeaders") or {},
            })
            m = re.search(r"TURN_NONCE=([0-9a-fA-F]{8})", prompt)
            nonce = m.group(1) if m else "00000000"

            if "SCENARIO_WARMING_UP_RETRY" in prompt and self.warmup_fail_remaining > 0:
                self.warmup_fail_remaining -= 1
                conn.sendall((json.dumps({"reqId": req_id, "event": "error", "code": "BRIDGE_WARMING_UP", "message": "WIZ hydrating"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_PREMATURE_EOF" in prompt and self.premature_eof_once:
                self.premature_eof_once = False
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "half chunk before socket drop"}) + "\n").encode("utf-8"))
                # Close socket WITHOUT `{"event": "done"}` to simulate AF_UNIX write drop!
                return

            if "SCENARIO_429_CARBON_STEPDOWN" in prompt:
                if self.http_429_remaining > 0:
                    self.http_429_remaining -= 1
                    conn.sendall((json.dumps({
                        "reqId": req_id,
                        "event": "error",
                        "code": "HTTP_429",
                        "message": "HTTP 429: )]}'\\n\\n55\\n[[\\\"wrb.fr\\\",null,null,null,null,[8,null,[[\\\"type.googleapis.com/assistant.lamda.BardFrontendService.BardErrorInfo\\\",[1095]]]]]]",
                    }) + "\n").encode("utf-8"))
                    return
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "thinking", "data": "429 降级接管推理", "conversation_id": "c_429_ok", "response_id": "r_429_ok", "choice_id": "rc_429_ok"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": f"429_STEPDOWN_RECOVERED_OK(m{slot_79}w{slot_80})", "conversation_id": "c_429_ok", "response_id": "r_429_ok", "choice_id": "rc_429_ok"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_FIRST_TOKEN_TIMEOUT" in prompt:
                if self.first_token_timeout_once:
                    self.first_token_timeout_once = False
                    conn.sendall((json.dumps({
                        "reqId": req_id,
                        "event": "error",
                        "code": "FIRST_TOKEN_TIMEOUT",
                        "message": "StreamGenerate produced zero wrb.fr frames within 35000ms (BOQ heartbeat stall)",
                    }) + "\n").encode("utf-8"))
                    return
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "FIRST_TOKEN_TIMEOUT_RECOVERED_OK", "conversation_id": "c_ftt", "response_id": "r_ftt", "choice_id": "rc_ftt"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_CKPT_TURN1" in prompt and "SCENARIO_CKPT_TURN2" not in prompt:
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "CKPT_TURN1_OK", "conversation_id": "c_ckpt_base", "response_id": "r_ckpt_1", "choice_id": "rc_ckpt_1"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_CKPT_TURN2" in prompt and "SCENARIO_CKPT_TURN3_1097" not in prompt:
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "CKPT_TURN2_STALE_CURSOR", "conversation_id": "c_ckpt_base", "response_id": "r_ckpt_poison2", "choice_id": "rc_ckpt_poison2"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_CKPT_TURN3_1097" in prompt:
                if self.err_1097_on_cursor_once and cursor_in == ["c_ckpt_base", "r_ckpt_poison2", "rc_ckpt_poison2"]:
                    self.err_1097_on_cursor_once = False
                    conn.sendall((json.dumps({
                        "reqId": req_id,
                        "event": "error",
                        "code": "BARD_ERR_1097",
                        "message": 'Upstream StreamGenerate returned no wrb.fr candidates: [["wrb.fr",null,null,null,null,[8,null,[["type.googleapis.com/assistant.lamda.BardFrontendService.BardErrorInfo",[1097,["无法继续对话",2]]]]]]]',
                    }) + "\n").encode("utf-8"))
                    return
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": f"CKPT_REWIND_OK(cursor_rid={cursor_in[1]})", "conversation_id": "c_ckpt_base", "response_id": "r_ckpt_3", "choice_id": "rc_ckpt_3"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_CANNED_CURSOR_HEAL" in prompt and self.canned_err_on_cursor_once and any(cursor_in):
                self.canned_err_on_cursor_once = False
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "I seem to be encountering an error. Can I try something else for you?", "conversation_id": cursor_in[0], "response_id": "r_bad", "choice_id": "rc_bad"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return

            if "SCENARIO_PURE_LAYER_B" in prompt and "[Local MCP Tool Result" not in prompt:
                body_txt = "调用本地探针\n" + make_call_envelope(nonce, "dsh_local_mcp_probe", {"echo": "pure_b_ok"})
                thk_txt = "执行网关内嵌 Layer B 本地 stdio MCP 短路测试"
            elif "[Local MCP Tool Result" in prompt and "pure_b_ok" in prompt:
                body_txt = "本地免公网 MCP 短路返回成功：pure_b_ok！"
                thk_txt = "收到网关内部回填的本地 MCP 结果"
            elif "SCENARIO_MIXED_AB" in prompt and "HOST_READ_OK_MARKER" not in prompt:
                body_txt = (
                    make_call_envelope(nonce, "dsh_local_mcp_probe", {"echo": "stashed_b_777"})
                    + "\n"
                    + make_call_envelope(nonce, "Read", {"file_path": "DEMO_GUIDE.md"})
                )
                thk_txt = "同轮并发触发 Layer B 本地 MCP 与 Layer A 宿主工具"
            elif "HOST_READ_OK_MARKER" in prompt:
                has_stitched = "stashed_b_777" in prompt
                body_txt = f"混合批次缝合验证：stitched={has_stitched}"
                thk_txt = "校验暂存的 Layer B 结果是否已自动缝合"
            elif "[DSH_AUTO_PAGINATED page=" in prompt and "DSH_PAGE_GREP" not in prompt:
                pm = re.search(r"page_token=(pg_[0-9a-f]+)", prompt)
                ptok = pm.group(1) if pm else ""
                body_txt = make_call_envelope(nonce, "dsh_read_tool_page", {"page_token": ptok, "grep": "NEEDLE_TARGET_999"})
                thk_txt = "调用内置无损翻页检索 NEEDLE_TARGET_999"
            elif "DSH_PAGE_GREP" in prompt:
                found = "NEEDLE_TARGET_999" in prompt
                body_txt = f"48K 无损分页检索命中：{found}"
                thk_txt = "分页检索完成"
            elif "SCENARIO_EARLY_STOP" in prompt:
                env_es = make_call_envelope(nonce, "Read", {"file_path": "early_stop_target.py"})
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "thinking", "data": "早停思考", "conversation_id": "c_es_1", "response_id": "r_es_1", "choice_id": "rc_es_1"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "准备调用工具\n" + env_es, "conversation_id": "c_es_1", "response_id": "r_es_1", "choice_id": "rc_es_1"}) + "\n").encode("utf-8"))
                time.sleep(0.06)
                try:
                    conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "\nFAKE_HALLUCINATED_RESULT_SHOULD_NOT_APPEAR"}) + "\n").encode("utf-8"))
                    conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                except OSError:
                    pass
                return
            elif "SCENARIO_STREAM_80KB_WRITE" in prompt:
                big_html = "<!DOCTYPE html>\n<html><body>\n" + ('<div class="frame" data-msg="hello \\"pelican\\"">鹈鹕骑车高帧率视差层</div>\n' * 1350) + "</body></html>\n"
                raw_env = make_raw_envelope(nonce, "Write", [("file_path", "pelican_bicycle.html"), ("content", big_html)])
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "thinking", "data": "正在规划鹈鹕骑自行车的物理骨骼与视差背景...", "conversation_id": "c_w80", "response_id": "r_w80", "choice_id": "rc_w80"}) + "\n").encode("utf-8"))
                for pos in range(0, len(raw_env), 2048):
                    piece = raw_env[pos : pos + 2048]
                    conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": piece, "conversation_id": "c_w80", "response_id": "r_w80", "choice_id": "rc_w80"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return
            elif "[Gateway Protocol Self-Heal]" in prompt:
                body_txt = make_call_envelope(nonce, "Bash", {"command": "ls -la", "description": "Recovered after self-heal"})
                thk_txt = "收到网关协议自愈提示，重新输出合法工具信封"
            elif "SCENARIO_MALFORMED_SELF_HEAL" in prompt:
                ctag = "dsh_" + "call_" + nonce
                body_txt = f"<{ctag}>{{{{\"completely_broken_garbage_without_tool_or_keys!!!</{ctag}>"
                thk_txt = "首次尝试输出畸形信封"
            elif "SCENARIO_CARBON_TIMEOUT_STEPDOWN" in prompt:
                if self.carbon_timeout_once:
                    self.carbon_timeout_once = False
                    conn.sendall((json.dumps({"reqId": req_id, "event": "error", "code": "FETCH_TIMEOUT", "message": "StreamGenerate headers timed out"}) + "\n").encode("utf-8"))
                    return
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Carbon 深度思考"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "thinking", "data": "保持满血 4 Carbon High 深度思考不降级", "conversation_id": "c_carbon_hi_ok1", "response_id": "r_ch", "choice_id": "rc_ch"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": f"CARBON_HIGH_PRESERVED_OK(m{slot_79}w{slot_80})", "conversation_id": "c_carbon_hi_ok1", "response_id": "r_ch", "choice_id": "rc_ch"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return
            elif "<block>yes</block>" in prompt or "<block>no</block>" in prompt:
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "<block>no</block>", "conversation_id": "c_ephem_block_9999", "response_id": "r_eb", "choice_id": "rc_eb"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return
            elif "SCENARIO_DOWNGRADE_GUARD" in prompt:
                if slot_79 == 8:
                    # Simulate non-Ultra account silently downgrading mode=8 (`Carbon`) to `"3.5 Flash-Lite"`!
                    conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "3.5 Flash-Lite"}) + "\n").encode("utf-8"))
                    try:
                        conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "FLASH_LITE_DOWNGRADED_TEXT_MUST_BE_ABORTED", "conversation_id": "c_dg_bad", "response_id": "r_dg_bad", "choice_id": "rc_dg_bad"}) + "\n").encode("utf-8"))
                        conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                    except OSError:
                        pass
                    return
                # Rerouted to `slot_79 == 1, slot_80 == 2` (`4 Hydrogen High`)!
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "thinking", "data": "已秒切至 Gemini 4 Hydrogen High 显式高阶推理模式！", "conversation_id": "c_dg_h4", "response_id": "r_dg_h4", "choice_id": "rc_dg_h4"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": "4 Hydrogen High 无缝接管完成", "conversation_id": "c_dg_h4", "response_id": "r_dg_h4", "choice_id": "rc_dg_h4"}) + "\n").encode("utf-8"))
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                return
            elif "SCENARIO_CANCEL_STREAM" in prompt:
                conn.sendall((json.dumps({"reqId": req_id, "event": "meta", "routed_model": "4 Hydrogen"}) + "\n").encode("utf-8"))
                for idx in range(30):
                    if cancel_ev is not None and cancel_ev.is_set():
                        return
                    try:
                        conn.sendall((json.dumps({"reqId": req_id, "event": "direct_delta", "kind": "text", "data": f"chunk_{idx} ", "conversation_id": "c_can", "response_id": "r_can", "choice_id": "rc_can"}) + "\n").encode("utf-8"))
                    except OSError:
                        return
                    time.sleep(0.05)
                try:
                    conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
                except OSError:
                    pass
                return
            elif "SCENARIO_STOP_SEQ" in prompt:
                body_txt = "alpha beta STOP_HERE gamma delta"
                thk_txt = ""
            elif "SCENARIO_OAI_TOOL_CALL" in prompt and "OAI_TOOL_RESULT_OK" not in prompt:
                body_txt = make_call_envelope(nonce, "Read", {"file_path": "DEMO_GUIDE.md"})
                thk_txt = "OpenAI 函数调用推理"
            elif "OAI_TOOL_RESULT_OK" in prompt:
                body_txt = "OpenAI 多轮工具闭环完成：OAI_TOOL_RESULT_OK"
                thk_txt = "总结工具输出"
            elif "[Gateway Tool Choice Enforcement]" in prompt:
                body_txt = make_call_envelope(nonce, "Read", {"file_path": "README.md"})
                thk_txt = ""
            elif "SCENARIO_FORCED_TOOL_MISS" in prompt:
                body_txt = "我先直接回答文本而不调用工具"
                thk_txt = ""
            elif "SCENARIO_THINK_DISABLED" in prompt:
                body_txt = f"disabled_think_slot80={slot_80}"
                thk_txt = "SHOULD_NOT_BE_EMITTED_TO_CLIENT"
            else:
                has_tombstone = "DSH_TOMBSTONE" in prompt
                has_sys_note = "[System Note]: MID_TURN_GUARD_RULE" in prompt
                body_txt = f"常规流式应答完成 (tombstone={has_tombstone}, sys_note={has_sys_note})"
                thk_txt = "常规深度思考推理完成"

            frames = [
                ")]}'\n\n" + build_wrb_frame("c_e2e_1", "r_e2e_1", "rc_e2e_1", thk_txt[:4], "", routed_model="4 Hydrogen"),
                build_wrb_frame("c_e2e_1", "r_e2e_1", "rc_e2e_1", thk_txt, body_txt[: min(8, len(body_txt))]),
                build_wrb_frame("c_e2e_1", "r_e2e_1", "rc_e2e_1", "", body_txt),
            ]
            for fr in frames:
                if cancel_ev.is_set():
                    return
                try:
                    conn.sendall((json.dumps({"reqId": req_id, "event": "raw_chunk", "data": fr}) + "\n").encode("utf-8"))
                except OSError:
                    return
                time.sleep(0.004)
            try:
                conn.sendall((json.dumps({"reqId": req_id, "event": "done"}) + "\n").encode("utf-8"))
            except OSError:
                pass
        finally:
            if cancel_ev is not None:
                with self._cancel_lock:
                    self._cancel_events.pop(req_id, None)
            try:
                conn.close()
            except Exception:
                pass


def stage_preflight_env() -> dict:
    runtime_files = [
        "gateway_poc.py",
        "webkit_plane_client.py",
        "embedded_local_mcp.py",
        "streaming_envelope_parser.py",
        "dsh_local_tools_mcp.py",
        "dsh_webkit_bridge.swift",
        "cc-demo.sh",
    ]
    forbidden_patterns = ["agy acp", "agy-acp", '"agy"', "'agy'"]
    audited = 0
    for fname in runtime_files:
        fpath = os.path.join(PROJECT_DIR, fname)
        if not os.path.isfile(fpath):
            raise RuntimeError(f"Missing core v4.3 source file: {fname}")
        with open(fpath, "r", encoding="utf-8") as fp:
            lines = fp.readlines()
        for lineno, ln in enumerate(lines, start=1):
            stripped = ln.strip()
            if stripped.startswith("#") or stripped.startswith("//") or stripped.startswith("*") or "零" in stripped or "ZERO" in stripped:
                continue
            low = stripped.lower()
            for pat in forbidden_patterns:
                if pat in low:
                    raise RuntimeError(f"Forbidden ACP reference {pat!r} found in {fname}:{lineno}")
        audited += 1

    os.makedirs(BIN_DIR, exist_ok=True)
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    return {"files_audited_zero_acp": audited}


def stage_build_native() -> dict:
    c_out = os.path.join(BIN_DIR, "dsh-mcp-stdio")
    swift_out = os.path.join(BIN_DIR, "dsh-webkit-bridge")
    app_dir = os.path.join(BIN_DIR, "DSHWebKitBridge.app")
    app_macos_dir = os.path.join(app_dir, "Contents", "MacOS")
    app_bin = os.path.join(app_macos_dir, "dsh-webkit-bridge")
    app_plist = os.path.join(app_dir, "Contents", "Info.plist")
    launcher_app_dir = os.path.join(PROJECT_DIR, "一键启动 Claude Code.app")
    c_src = os.path.join(PROJECT_DIR, "dsh_mcp_launcher.c")
    swift_src = os.path.join(PROJECT_DIR, "dsh_webkit_bridge.swift")

    prev_manifest: dict = {}
    if os.path.isfile(MANIFEST_PATH):
        try:
            with open(MANIFEST_PATH, "r", encoding="utf-8") as fp:
                prev_manifest = json.load(fp)
        except Exception:
            prev_manifest = {}

    cur_c_sha = sha256_file(c_src)
    cur_swift_sha = sha256_file(swift_src)
    rebuilt_targets: List[str] = []

    if (
        not (os.path.isfile(c_out) and os.access(c_out, os.X_OK))
        or prev_manifest.get("dsh-mcp-stdio_sha256") != cur_c_sha
    ) and os.path.isfile("/usr/bin/clang"):
        subprocess.run(["/usr/bin/clang", "-O2", c_src, "-o", c_out], check=True, capture_output=True, text=True)
        rebuilt_targets.append("dsh-mcp-stdio")

    need_swift_build = (
        not (os.path.isfile(swift_out) and os.access(swift_out, os.X_OK))
        or not (os.path.isfile(app_bin) and os.access(app_bin, os.X_OK))
        or prev_manifest.get("dsh-webkit-bridge_sha256") != cur_swift_sha
        or prev_manifest.get("version") != TARGET_VERSION
    )
    if need_swift_build and os.path.isfile("/usr/bin/swiftc"):
        os.makedirs(app_macos_dir, exist_ok=True)
        subprocess.run(
            ["/usr/bin/swiftc", "-O", "-framework", "AppKit", "-framework", "WebKit", swift_src, "-o", swift_out],
            check=True,
            capture_output=True,
            text=True,
        )
        shutil.copy2(swift_out, app_bin)
        os.chmod(app_bin, 0o755)
        subprocess.run(
            ["/usr/bin/codesign", "--force", "--deep", "--sign", "-", app_dir],
            check=True,
            capture_output=True,
            text=True,
        )
        if os.path.isdir(launcher_app_dir):
            subprocess.run(
                ["/usr/bin/codesign", "--force", "--deep", "--sign", "-", launcher_app_dir],
                check=True,
                capture_output=True,
                text=True,
            )
        rebuilt_targets.append("dsh-webkit-bridge+app")

    for req_exe in (c_out, swift_out, app_bin):
        if not (os.path.isfile(req_exe) and os.access(req_exe, os.X_OK)):
            raise RuntimeError(f"Missing executable {req_exe}")

    file_out = subprocess.run(["/usr/bin/file", app_bin], check=True, capture_output=True, text=True).stdout.strip()
    assert "Mach-O" in file_out, f"Invalid binary format: {file_out}"
    subprocess.run(["/usr/bin/codesign", "--verify", "--deep", "--strict", app_dir], check=True, capture_output=True, text=True)
    with open(app_plist, "r", encoding="utf-8") as fp:
        plist_txt = fp.read()
    assert "4.6.0" in plist_txt, f"Expected 4.6.0 in {app_plist}"

    manifest = {
        "version": TARGET_VERSION,
        "dsh-mcp-stdio_sha256": cur_c_sha,
        "dsh-webkit-bridge_sha256": cur_swift_sha,
        "webkit_binary_bytes": os.path.getsize(app_bin),
        "mcp_binary_bytes": os.path.getsize(c_out),
        "macho_arch": file_out.split(":")[-1].strip(),
        "codesign_verified": True,
        "rebuilt_targets": rebuilt_targets,
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2)
    return manifest


def stage_unit_fsm_and_mcp() -> dict:
    sys.path.insert(0, PROJECT_DIR)
    import gateway_poc as gw
    from embedded_local_mcp import EmbeddedLocalMcpHost
    from streaming_envelope_parser import (
        StreamingEnvelopeParser,
        generate_turn_nonce,
        lexical_repair_json,
        schema_anchor_extract,
        stateful_repair_json,
    )
    from webkit_plane_client import WebProtoProfileCache, WrbStreamFrameDecoder, build_stream_generate_freq

    nonce = generate_turn_nonce("plain outbound sample")
    assert len(nonce) == 8

    # 0. Non-fracturing explicit `derive_conv_key` across varying first-message reminders + background utility isolation
    k1 = gw.derive_conv_key({"x-claude-code-session-id": "089c1268-37e7-4d77-a94f-9b628fad5ea3"}, {}, "", [{"role": "user", "content": "hello"}])
    k2 = gw.derive_conv_key({"x-claude-code-session-id": "089c1268-37e7-4d77-a94f-9b628fad5ea3"}, {}, "", [{"role": "user", "content": "compacted summary + hello"}])
    k_bg = gw.derive_conv_key({"x-claude-code-session-id": "089c1268-37e7-4d77-a94f-9b628fad5ea3", "x-claude-code-request-class": "utility"}, {}, "", [{"role": "user", "content": "summarize"}])
    assert k1 == k2 == "sid:089c1268-37e7-4d77-a94f-9b628fad5ea3"
    assert k_bg == "sid:089c1268-37e7-4d77-a94f-9b628fad5ea3:cls:utility"

    # 0b. `# Environment` mid-stream `role: "system"` invariance (Turn 1 trailing vs Turn 2 pre-tool_result -> identical `sys_out`!)
    sys_t1, msgs_t1 = gw.fold_mid_stream_system_messages("Base sys", [
        {"role": "user", "content": "Run test"},
        {"role": "system", "content": "# Environment\nPrimary working directory: /tmp"},
    ])
    sys_t2, msgs_t2 = gw.fold_mid_stream_system_messages("Base sys", [
        {"role": "user", "content": "Run test"},
        {"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]},
        {"role": "system", "content": "# Environment\nPrimary working directory: /tmp"},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}]},
    ])
    assert sys_t1 == sys_t2 and "# Environment" in sys_t1
    assert len(msgs_t1) == 1 and len(msgs_t2) == 3 and all(m.get("role") != "system" for m in msgs_t2)

    # 1. 1-byte split + `<dsh_call_EXAMPLE>` literal immunity
    parser = StreamingEnvelopeParser(nonce=nonce)
    env_1 = make_call_envelope(nonce, "Read", {"file_path": "README.md"})
    stream_sample = f"正在读取说明文件 (示例 `<dsh_call_EXAMPLE>`)...\n{env_1}\n读取请求已发出。"
    emitted: List[Tuple[str, Any]] = []
    for ch in stream_sample:
        emitted.extend(parser.feed_text(ch))
    emitted.extend(parser.finish())

    text_joined = "".join(v for k, v in emitted if k == "text")
    calls = [v for k, v in emitted if k == "tool_call"]
    assert nonce not in text_joined and "<dsh_call_EXAMPLE>" in text_joined
    assert len(calls) == 1 and calls[0]["name"] == "Read" and calls[0]["input"]["file_path"] == "README.md"

    # 2. Blocking code-fence immunity
    parser_fence = StreamingEnvelopeParser(nonce=nonce, respect_code_fences=True)
    fake_in_fence = make_call_envelope(nonce, "Write", {"file_path": "ignored.txt", "content": "x"})
    real_outside = make_call_envelope(nonce, "Read", {"file_path": "DEMO_GUIDE.md"})
    fenced_doc = f"文档示例：\n```python\n# {fake_in_fence}\nvalue = 1\n```\n{real_outside}"
    ev_f = parser_fence.feed_text(fenced_doc) + parser_fence.finish()
    calls_f = [v for k, v in ev_f if k == "tool_call"]
    assert len(calls_f) == 1 and calls_f[0]["name"] == "Read"

    # 3. 120KB RAW mode + permissive `<dsh_call tool='Write' mode=raw><arg name=file_path>`
    parser_raw = StreamingEnvelopeParser(nonce=nonce)
    complex_code = ('def calc(x="a\\"b", pat=r"\\d+\\s*"):\n    return """multi\nline"""\n' * 2200)
    raw_envelope = make_raw_envelope(nonce, "Write", [("file_path", "sample.py"), ("content", complex_code)])
    ev_r: List[Tuple[str, Any]] = []
    for i in range(0, len(raw_envelope), 509):
        ev_r.extend(parser_raw.feed_text(raw_envelope[i : i + 509]))
    ev_r.extend(parser_raw.finish())
    calls_r = [v for k, v in ev_r if k == "tool_call"]
    assert len(calls_r) == 1 and calls_r[0]["name"] == "Write"
    assert calls_r[0]["input"]["content"] == complex_code

    perm_parser = StreamingEnvelopeParser(nonce=nonce, cwd="/Users/demo/proj")
    perm_env = "<dsh_call tool='Write' mode=raw><arg name=file_path>sub/app.js</arg><arg name='content'>const x = 1;</arg></dsh_call>"
    perm_calls = [v for k, v in (perm_parser.feed_text(perm_env) + perm_parser.finish()) if k == "tool_call"]
    assert len(perm_calls) == 1 and perm_calls[0]["input"]["file_path"] == "/Users/demo/proj/sub/app.js"
    assert perm_calls[0]["input"]["content"] == "const x = 1;"

    # 4. 3-Layer JSON Recovery
    broken_json = '{"name": "Read", "input": {"file_path": "a\\d+\nb.txt",},}'
    repaired = lexical_repair_json(broken_json)
    assert isinstance(repaired, dict) and repaired["name"] == "Read"

    quote_colon_json = '{"name": "Bash", "input": {"command": "grep "error: fatal" server.log", "description": "Search "error: fatal" logs",}}'
    rep_l2 = stateful_repair_json(quote_colon_json)
    assert isinstance(rep_l2, dict) and rep_l2["input"]["command"] == 'grep "error: fatal" server.log'

    mangled_anchor = '{name: "Bash", "input": {"command": "ls -la /tmp", "description": "list tmp"'
    rep_l3 = schema_anchor_extract(mangled_anchor)
    assert isinstance(rep_l3, dict) and rep_l3["name"] == "Bash" and rep_l3["input"]["command"] == "ls -la /tmp"

    # 5. V8 Duplicate-Key Poison-Pill Seal on truncated mid-stream RAW Write
    trunc_parser = StreamingEnvelopeParser(nonce=nonce, stream_raw_tool_names={"Write"}, cwd="/Users/demo")
    half_raw = f'<dsh_call_{nonce} tool="Write" mode="raw"><dsh_arg_{nonce} name="file_path">a.html</dsh_arg_{nonce}><dsh_arg_{nonce} name="content"><html>half'
    trunc_evs = trunc_parser.feed_text(half_raw) + trunc_parser.finish()
    pjson_joined = "".join(v["partial_json"] for k, v in trunc_evs if k == "tool_delta")
    sealed_obj = json.loads(pjson_joined)
    assert sealed_obj["file_path"] is None and sealed_obj["__dsh_incomplete__"] is True

    # 6. 3-Path WrbStreamFrameDecoder + Stateful Canvas `cand[30]` inline expansion
    wdec = WrbStreamFrameDecoder()
    raw_http = (
        ")]}'\n\n"
        + build_wrb_frame("c_abc", "r_111", "rc_222", "先思考第一步", "")
        + build_wrb_frame("c_abc", "r_111", "rc_222", "先思考第一步，再确认目录", "你好")
        + build_wrb_frame("c_abc", "r_111", "rc_222", "", "你好，世界！")
    )
    w_events: List[Tuple[str, str]] = []
    for step in range(0, len(raw_http), 17):
        w_events.extend(wdec.feed_raw_chunk(raw_http[step : step + 17]))
    assert "".join(v for k, v in w_events if k == "thinking") == "先思考第一步，再确认目录"
    assert "".join(v for k, v in w_events if k == "text") == "你好，世界！"
    assert wdec.cursor() == ("c_abc", "r_111", "rc_222")

    wdec_canvas = WrbStreamFrameDecoder()
    canvas_entry = [["cid_0", None, "Pelican Demo", None, "<!DOCTYPE html><h1>Pelican</h1>", None, None, None, None, "pelican.html", 2]]
    canvas_stream = (
        ")]}'\n\n"
        + build_wrb_frame("c_cv", "r_cv", "rc_cv", "", "为您生成代码：\nhttp://googleusercontent.com/immersive_entry_chip/0", step_thoughts=["步骤一：构思结构", "步骤二：编写渲染"], canvas_entries=canvas_entry)
        + build_wrb_frame("c_cv", "r_cv", "rc_cv", "", "为您生成代码：\nhttp://googleusercontent.com/immersive_entry_chip/0\n生成完毕！", canvas_entries=None)
    )
    cv_evs = wdec_canvas.feed_raw_chunk(canvas_stream)
    cv_thk = "".join(v for k, v in cv_evs if k == "thinking")
    cv_txt = "".join(v for k, v in cv_evs if k == "text")
    assert "步骤一：构思结构" in cv_thk and "步骤二：编写渲染" in cv_thk
    assert "immersive_entry_chip" not in cv_txt and "```html\n<!DOCTYPE html><h1>Pelican</h1>\n```" in cv_txt

    # 7. Nov 2026 `Carbon` (`mode=8`) + `4 Hydrogen High` (`mode=1, ThinkWire=2`) Routing & 18-Elem `525001261-jspb`
    tmp_proto = os.path.join(RUNTIME_DIR, f"test_proto_{os.getpid()}.json")
    try:
        with open(tmp_proto, "w", encoding="utf-8") as pfp:
            json.dump({
                "bl": "boq_gemini-web-uiserver_20261115.13_p0",
                "capacity": [4],
                "extra_headers": {
                    "Cookie": "evil=1",
                    "x-goog-ext-525001261-jspb": '[1,null,null,null,null,null,"zh-CN",null,[4,33,22,5,6,8,16,19,20,38],null,null,null,null,null,null,null,"UUID",["boq_gemini-web-uiserver_20261115.13_p0"]]',
                },
                "policy_models": [
                    {"label": "4 Hydrogen", "enabled": True},
                    {"label": "4 Argon", "enabled": False},
                    {"label": "4 Carbon", "enabled": True},
                    {"label": "3.5 Flash-Lite", "enabled": True},
                ],
                "discovered_models": {
                    "Model_1bc6b5": {"hex": "1bc6b5d98741cd3d", "label": "Model_1bc6b5", "mode": 1},  # Poisoned disabled 4 Argon hex FIRST in dict!
                    "Carbon": {"hex": "95a4c0abcba24364", "label": "Carbon", "mode": 1},  # Even if legacy walkNode wrote mode=1!
                    "4 Hydrogen": {"hex": "56fdd199312815e2", "label": "4 Hydrogen", "mode": 1},
                    "Flash-Lite": {"hex": "1d44b34bcaa1c04d", "label": "Flash-Lite", "mode": 6},
                },
            }, pfp)
        pcache_nov = WebProtoProfileCache(tmp_proto)
        # Verify `Model_1bc6b5` (`1bc6b5d98741cd3d`, disabled `4 Argon`) NEVER hijacks `gemini-app-thinking` (`4 Hydrogen` -> `56fdd199312815e2`) or `gemini-app-carbon` (`95a4c0abcba24364`)!
        hdrs_thk_guard = pcache_nov.resolve_extra_headers("gemini-app-thinking", tier="high")
        ext_thk_guard = json.loads(hdrs_thk_guard["x-goog-ext-525001261-jspb"])
        assert ext_thk_guard[4] == "56fdd199312815e2", f"Model_1bc6b5 hijacked thinking hex: {ext_thk_guard[4]}"
        freq_c = json.loads(build_stream_generate_freq("hi", turn_index=2, model_id="gemini-app-carbon", tier="high", proto_cache=pcache_nov))
        inner_c = json.loads(freq_c[1])
        hdrs_c = pcache_nov.resolve_extra_headers("gemini-app-carbon", tier="high")
        ext_c = json.loads(hdrs_c["x-goog-ext-525001261-jspb"])
        assert "Cookie" not in hdrs_c
        assert inner_c[17] == [[2]] and inner_c[32] == 1 and inner_c[79] == 8 and inner_c[80] == 2
        assert ext_c[14] == 8 and ext_c[4] == "95a4c0abcba24364" and len(ext_c[8]) == 10 and ext_c[17] == ["boq_gemini-web-uiserver_20261115.13_p0"]

        # When mode=8 is marked downgraded on non-Ultra accounts, `gemini-app-carbon` & `gemini-app-thinking` route directly to `4 Hydrogen High` (`(1, 2)`) — NEVER retired `3.1 Pro`!
        pcache_nov.mark_mode_downgraded(8)
        freq_h = json.loads(build_stream_generate_freq("hi", turn_index=0, model_id="gemini-app-carbon", tier="high", proto_cache=pcache_nov))
        inner_h = json.loads(freq_h[1])
        ext_h = json.loads(pcache_nov.resolve_extra_headers("gemini-app-carbon", tier="high")["x-goog-ext-525001261-jspb"])
        assert inner_h[79] == 1 and inner_h[80] == 2 and ext_h[14] == 1 and ext_h[15] == 2 and ext_h[4] == "56fdd199312815e2"

        # Verify v4.4 THINK_WIRE semantics (`low=1`, `medium=4`, `high=2`, `xhigh=2`) in both f.req[80] and ext[15]
        for tier_name, exp_wire in (("low", 1), ("medium", 4), ("high", 2), ("xhigh", 2)):
            fr_t = json.loads(build_stream_generate_freq("hi", turn_index=0, model_id="gemini-app-thinking", tier=tier_name, proto_cache=pcache_nov))
            in_t = json.loads(fr_t[1])
            ex_t = json.loads(pcache_nov.resolve_extra_headers("gemini-app-thinking", tier=tier_name)["x-goog-ext-525001261-jspb"])
            assert in_t[80] == exp_wire and ex_t[15] == exp_wire, f"{tier_name}: {in_t[80]} / {ex_t[15]}"

        # Verify v4.4 clean text preservation (no "Claude" -> "the assistant" / "Anthropic" -> "the provider" corruption)
        assert gw.sanitize_outbound_text("Claude and Anthropic stay intact") == "Claude and Anthropic stay intact"

        # Verify nonced `<dsh_arg_{nonce}>` does not falsely close on `</param>` inside content
        p_nonced = StreamingEnvelopeParser(nonce=nonce)
        nonced_env = f'<dsh_call_{nonce} tool="Write" mode="raw"><dsh_arg_{nonce} name="file_path">x.xml</dsh_arg_{nonce}><dsh_arg_{nonce} name="content"><a></param><b></dsh_arg_{nonce}></dsh_call_{nonce}>'
        nc_calls = [v for k, v in (p_nonced.feed_text(nonced_env) + p_nonced.finish()) if k == "tool_call"]
        assert len(nc_calls) == 1 and nc_calls[0]["input"]["content"] == "<a></param><b>"
    finally:
        for p in (tmp_proto, tmp_proto + ".lock"):
            if os.path.exists(p):
                os.unlink(p)

    host = EmbeddedLocalMcpHost(base_config_paths=[os.path.join(PROJECT_DIR, "gemini_local_mcp_config.json")])
    try:
        b_tools = host.get_active_layer_b_tools(client_layer_a_names={"Read"})
        assert any(t["name"] == "dsh_local_mcp_probe" for t in b_tools)
        probe_out = host.execute_layer_b_tool("dsh_local_mcp_probe", {"echo": "unit_test_123"}, {"Read"})
        assert probe_out is not None and not probe_out["is_error"]
        probe_json = json.loads(probe_out["text"])
        assert probe_json["ok"] is True and probe_json["echo"] == "unit_test_123"
    finally:
        host.shutdown_all()

    return {
        "session_key_stability": "PASS (explicit session-id never fractures)",
        "fsm_1byte_and_example_guard": "PASS",
        "fence_immunity": "PASS",
        "raw_120kb_and_permissive_tags": "PASS",
        "three_layer_json_recovery": "PASS (KEY/VAL quotes + anchor extract)",
        "v8_poison_pill_truncation_seal": "PASS",
        "three_path_thinking_and_canvas": "PASS",
        "nov2026_carbon_hydrogen_routing": "PASS (Carbon mode=8 -> 4 Hydrogen High mode=1,w=2, 18-elem header)",
        "embedded_stdio_mcp": "PASS",
    }


def stage_protocol_e2e() -> dict:
    sys.path.insert(0, PROJECT_DIR)
    import gateway_poc as gw
    from webkit_plane_client import WebKitPlaneClient

    os.environ["DSH_QUIET"] = "1"
    tmp_sess = os.path.join(RUNTIME_DIR, f"test_sess_e2e_{os.getpid()}.json")
    tmp_acct = os.path.join(RUNTIME_DIR, f"test_acct_e2e_{os.getpid()}.json")
    peer = MockWebKitStreamPeer()
    mock_plane = WebKitPlaneClient(
        socket_path=gw.WEBKIT_SOCK,
        binary_path=gw.WEBKIT_BIN,
        auto_spawn=False,
        socket_connector=peer.create_connected_socket,
    )

    orig_wm = gw.SOFT_WATERMARK_TOKENS
    orig_hwm = gw.HARD_CEILING_TOKENS
    orig_plane = gw.WEBKIT_PLANE
    orig_cstore = gw.REGISTRY.cursor_store
    orig_astore = gw.ACCOUNT_STORE

    gw.SOFT_WATERMARK_TOKENS = 2000
    gw.HARD_CEILING_TOKENS = 4000
    gw.WEBKIT_PLANE = mock_plane
    gw.REGISTRY.cursor_store = gw.SessionCursorStore(tmp_sess)
    gw.ACCOUNT_STORE = gw.AccountAlignmentStore(tmp_acct)
    gw.ACCOUNT_STORE.set_pinned("")
    gw.ACCOUNT_STORE.set_pinned("")

    client = SocketPairHTTPClient(gw.GatewayHTTPHandler)

    try:
        code_h, _, _ = client.request("HEAD", "/api/hello")
        assert code_h == 200
        reqs_before = len(peer.requests_log)
        for fp_msg, max_t in [("ping", 1), ("Generate isNewTopic JSON", 128)]:
            c_fp, h_fp, _ = client.request(
                "POST",
                "/v1/messages",
                {"model": "gemini-app-fast", "max_tokens": max_t, "messages": [{"role": "user", "content": fp_msg}]},
                {"User-Agent": "claude-cli/2.1.325"},
            )
            assert c_fp == 200 and h_fp.get("x-turn-mode") == "fast_path"
        assert len(peer.requests_log) == reqs_before

        host_tools = [
            {"name": "Read", "description": "Read file", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
            {"name": "Write", "description": "Write file", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}, "content": {"type": "string"}}}},
            {"name": "Bash", "description": "Run bash", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}, "description": {"type": "string"}}}},
        ]

        # 1. Pure Layer-B Zero-Public-IP Local MCP Short-Circuit
        c_b, _, sse_b = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": True,
                "max_tokens": 4096,
                "tools": host_tools,
                "messages": [{"role": "user", "content": "SCENARIO_PURE_LAYER_B"}],
            },
            {"x-claude-code-session-id": "sess-pure-b"},
        )
        assert c_b == 200 and ": mcp-local-executing" in sse_b and "pure_b_ok" in sse_b and '"signature_delta"' in sse_b

        # 2. Mixed Layer A + Layer B Tool Batch & Result Stitching
        c_m1, _, raw_m1 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "tools": host_tools,
                "messages": [{"role": "user", "content": "SCENARIO_MIXED_AB"}],
            },
            {"x-claude-code-session-id": "sess-mixed-ab"},
        )
        msg_m1 = json.loads(raw_m1)
        assert c_m1 == 200 and msg_m1["stop_reason"] == "tool_use"
        tool_blocks = [b for b in msg_m1["content"] if b["type"] == "tool_use"]
        assert len(tool_blocks) == 1 and tool_blocks[0]["name"] == "Read"
        read_call_id = tool_blocks[0]["id"]

        c_m2, _, raw_m2 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "tools": host_tools,
                "messages": [
                    {"role": "user", "content": "SCENARIO_MIXED_AB"},
                    {"role": "assistant", "content": msg_m1["content"]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": read_call_id, "content": "HOST_READ_OK_MARKER"}]},
                ],
            },
            {"x-claude-code-session-id": "sess-mixed-ab"},
        )
        msg_m2 = json.loads(raw_m2)
        txt_m2 = "".join(b.get("text", "") for b in msg_m2["content"] if b["type"] == "text")
        assert "stitched=True" in txt_m2

        # 3. 48K Lossless Tool Pagination
        huge_log = ("def helper_alpha():\n    pass\n" + ("INFO normal line\n" * 3600) + "CRITICAL NEEDLE_TARGET_999 hit!\n" + ("TAIL line\n" * 800))
        c_p, _, raw_p = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-fast",
                "stream": False,
                "tools": host_tools,
                "messages": [
                    {"role": "user", "content": "Read huge log"},
                    {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_pg1", "name": "Read", "input": {"file_path": "app.log"}}]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_pg1", "content": huge_log}]},
                ],
            },
            {"x-claude-code-session-id": "sess-paginate"},
        )
        msg_p = json.loads(raw_p)
        txt_p = "".join(b.get("text", "") for b in msg_p["content"] if b["type"] == "text")
        assert "48K 无损分页检索命中：True" in txt_p

        # 4. Soft-Watermark Tombstone Rollover
        filler = "x" * 9000
        client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "Turn 1"},
                    {"role": "assistant", "content": [{"type": "tool_use", "id": "t_old", "name": "Read", "input": {"file_path": "a"}}]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t_old", "content": filler}]},
                ],
            },
            {"x-claude-code-session-id": "sess-rollover"},
        )
        _, h_r2, raw_r2 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "Turn 1"},
                    {"role": "assistant", "content": [{"type": "tool_use", "id": "t_old", "name": "Read", "input": {"file_path": "a"}}]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t_old", "content": filler}]},
                    {"role": "assistant", "content": [{"type": "text", "text": "ok"}]},
                    {"role": "user", "content": "Turn 2 trigger watermark " + ("y" * 4000)},
                    {"role": "assistant", "content": [{"type": "text", "text": "ack"}]},
                    {"role": "user", "content": "Turn 3 after rollover"},
                ],
            },
            {"x-claude-code-session-id": "sess-rollover"},
        )
        assert h_r2.get("x-turn-mode") == "rollover_256k_tombstone" and "tombstone=True" in raw_r2

        # Verify non-thrashing: next turn stays sticky_delta instead of endlessly rolling over
        c_r3, h_r3, _ = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "Turn 1"},
                    {"role": "assistant", "content": [{"type": "tool_use", "id": "t_old", "name": "Read", "input": {"file_path": "a"}}]},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t_old", "content": filler}]},
                    {"role": "assistant", "content": [{"type": "text", "text": "ok"}]},
                    {"role": "user", "content": "Turn 2 trigger watermark " + ("y" * 4000)},
                    {"role": "assistant", "content": [{"type": "text", "text": "ack"}]},
                    {"role": "user", "content": "Turn 3 after rollover"},
                    {"role": "assistant", "content": [{"type": "text", "text": json.loads(raw_r2)["content"][-1]["text"]}]},
                    {"role": "user", "content": "Turn 4 sticky check after rollover"},
                ],
            },
            {"x-claude-code-session-id": "sess-rollover"},
        )
        assert c_r3 == 200 and h_r3.get("x-turn-mode") == "sticky_delta", f"Rollover thrashed: {h_r3}"

        # 5. Account Alignment Guard (v4.4 enforces HTTP 403 pre-flight before streaming)
        client.request("POST", "/_admin/pin_account", {"account": "other.app@gmail.com"})
        c_g1, _, raw_g1 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "tools": host_tools, "messages": [{"role": "user", "content": "Test mismatch guard"}]},
            {"x-claude-code-session-id": "sess-acct-guard"},
        )
        err_g1 = json.loads(raw_g1)
        assert c_g1 == 403 and (err_g1.get("error") or {}).get("type") == "permission_error" and "Account alignment blocked" in (err_g1.get("error") or {}).get("message", ""), f"Got {c_g1}: {raw_g1}"
        client.request("POST", "/_admin/pin_account", {"account": ""})

        # 6. Direct-Delta + Early-Stop Tool Call Truncation
        c_es, _, raw_es = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "tools": host_tools, "messages": [{"role": "user", "content": "SCENARIO_EARLY_STOP"}]},
            {"x-claude-code-session-id": "sess-early-stop"},
        )
        msg_es = json.loads(raw_es)
        es_txt = "".join(b.get("text", "") for b in msg_es["content"] if b["type"] == "text")
        es_tools = [b for b in msg_es["content"] if b["type"] == "tool_use"]
        assert c_es == 200 and len(es_tools) == 1 and es_tools[0]["input"]["file_path"] == "early_stop_target.py"
        assert "FAKE_HALLUCINATED_RESULT_SHOULD_NOT_APPEAR" not in es_txt

        # 7. Real-Time >80KB `Write` `input_json_delta` Streaming (Zero `8 KB` Socket Cutoff!)
        c_w80, _, sse_w80 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": True,
                "system": f"Primary working directory: {PROJECT_DIR}",
                "tools": host_tools,
                "messages": [{"role": "user", "content": "SCENARIO_STREAM_80KB_WRITE"}],
            },
            {"x-claude-code-session-id": "sess-stream-80kb"},
        )
        assert c_w80 == 200
        json_deltas: List[str] = []
        thinking_deltas: List[str] = []
        tool_starts = 0
        for ln in sse_w80.splitlines():
            if ln.startswith("data: "):
                ev_obj = json.loads(ln[6:])
                if ev_obj.get("type") == "content_block_start" and (ev_obj.get("content_block") or {}).get("type") == "tool_use":
                    tool_starts += 1
                elif ev_obj.get("type") == "content_block_delta":
                    d = ev_obj.get("delta") or {}
                    if d.get("type") == "input_json_delta":
                        json_deltas.append(d["partial_json"])
                    elif d.get("type") == "thinking_delta":
                        thinking_deltas.append(d["thinking"])
        assert tool_starts == 1, f"Expected 1 streamed tool_use block, got {tool_starts}"
        assert len(json_deltas) >= 20, f"Expected >=20 live JSON delta chunks, got {len(json_deltas)}"
        assert len(thinking_deltas) >= 1
        assembled_write = json.loads("".join(json_deltas))
        assert assembled_write["file_path"] == os.path.join(PROJECT_DIR, "pelican_bicycle.html")
        assert len(assembled_write["content"]) > 80000

        # 8. Gateway-Internal Self-Heal Retry Hop
        c_sh, _, sse_sh = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": True,
                "tools": host_tools,
                "messages": [{"role": "user", "content": "SCENARIO_MALFORMED_SELF_HEAL"}],
            },
            {"x-claude-code-session-id": "sess-self-heal"},
        )
        assert c_sh == 200
        assert ": dsh-envelope-self-heal" in sse_sh
        assert "skipped malformed tool call envelope" not in sse_sh
        assert '"stop_reason": "tool_use"' in sse_sh
        assert "Recovered after self-heal" in sse_sh

        # 9. v4.4 API Compliance: 400 Validation, stop_sequences, thinking.disabled, tool_choice, OpenAI Chat/Responses, Cancel
        c_bad_empty, _, _ = client.request("POST", "/v1/messages", {"model": "gemini-app-carbon", "messages": []})
        assert c_bad_empty == 400
        c_bad_mm, _, _ = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "messages": [{"role": "user", "content": [{"type": "image", "source": {}}]}]},
        )
        assert c_bad_mm == 400

        c_ss, _, raw_ss = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "stop_sequences": ["STOP_HERE"],
                "messages": [{"role": "user", "content": "SCENARIO_STOP_SEQ"}],
            },
            {"x-claude-code-session-id": "sess-stop-seq"},
        )
        msg_ss = json.loads(raw_ss)
        assert c_ss == 200 and msg_ss["stop_reason"] == "stop_sequence" and msg_ss["stop_sequence"] == "STOP_HERE"
        assert msg_ss["content"][0]["text"] == "alpha beta "

        c_td, _, raw_td = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "thinking": {"type": "disabled"},
                "messages": [{"role": "user", "content": "SCENARIO_THINK_DISABLED"}],
            },
            {"x-claude-code-session-id": "sess-think-disabled"},
        )
        msg_td = json.loads(raw_td)
        assert c_td == 200 and not any(b.get("type") == "thinking" for b in msg_td["content"])
        assert "disabled_think_slot80=1" in msg_td["content"][0]["text"]

        c_tc, _, raw_tc = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "tools": host_tools,
                "tool_choice": {"type": "tool", "name": "Read"},
                "messages": [{"role": "user", "content": "SCENARIO_FORCED_TOOL_MISS"}],
            },
            {"x-claude-code-session-id": "sess-tool-choice"},
        )
        msg_tc = json.loads(raw_tc)
        assert c_tc == 200 and msg_tc["stop_reason"] == "tool_use"
        assert any(b.get("type") == "tool_use" and b.get("name") == "Read" for b in msg_tc["content"])

        # OpenAI /v1/chat/completions streaming tool_calls + multi-turn role:"tool"
        oai_tools = [{"type": "function", "function": {"name": "Read", "description": "Read", "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}}}}}]
        c_oai1, _, sse_oai1 = client.request(
            "POST",
            "/v1/chat/completions",
            {
                "model": "gemini-app-carbon",
                "stream": True,
                "stream_options": {"include_usage": True},
                "tools": oai_tools,
                "messages": [{"role": "user", "content": "SCENARIO_OAI_TOOL_CALL"}],
            },
            {"x-claude-code-session-id": "sess-oai-tools"},
        )
        assert c_oai1 == 200 and '"tool_calls"' in sse_oai1 and '"finish_reason": "tool_calls"' in sse_oai1 and '"total_tokens"' in sse_oai1
        c_oai2, _, raw_oai2 = client.request(
            "POST",
            "/v1/chat/completions",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "tools": oai_tools,
                "messages": [
                    {"role": "user", "content": "SCENARIO_OAI_TOOL_CALL"},
                    {"role": "assistant", "content": None, "tool_calls": [{"id": "call_read_1", "type": "function", "function": {"name": "Read", "arguments": "{\"file_path\":\"DEMO_GUIDE.md\"}"}}]},
                    {"role": "tool", "tool_call_id": "call_read_1", "content": "OAI_TOOL_RESULT_OK"},
                ],
            },
            {"x-claude-code-session-id": "sess-oai-tools"},
        )
        assert c_oai2 == 200 and "OAI_TOOL_RESULT_OK" in json.loads(raw_oai2)["choices"][0]["message"]["content"]

        # OpenAI /v1/responses streaming
        c_resp, _, sse_resp = client.request(
            "POST",
            "/v1/responses",
            {"model": "gemini-app-fast", "stream": True, "input": "Hello responses API"},
            {"x-claude-code-session-id": "sess-oai-resp"},
        )
        assert c_resp == 200 and "response.output_text.delta" in sse_resp and "response.completed" in sse_resp

        # Cancel propagation on mid-stream client disconnect
        c_sock_can, s_sock_can = socket.socketpair()
        srv_dummy = type("DummyServer", (), {"server_name": "localhost", "server_port": 18896})()
        t_can = threading.Thread(target=gw.GatewayHTTPHandler, args=(s_sock_can, ("127.0.0.1", 0), srv_dummy), daemon=True)
        t_can.start()
        can_payload = json.dumps({"model": "gemini-app-carbon", "stream": True, "messages": [{"role": "user", "content": "SCENARIO_CANCEL_STREAM"}]}).encode("utf-8")
        c_sock_can.sendall(
            f"POST /v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nX-Claude-Code-Session-Id: sess-cancel\r\nContent-Length: {len(can_payload)}\r\n\r\n".encode("utf-8") + can_payload
        )
        recv_so_far = b""
        while b"chunk_1" not in recv_so_far and len(recv_so_far) < 16384:
            part = c_sock_can.recv(4096)
            if not part:
                break
            recv_so_far += part
        c_sock_can.close()
        t_can.join(timeout=4.0)
        try:
            s_sock_can.close()
        except Exception:
            pass
        for _ in range(20):
            if peer.cancelled_req_ids:
                break
            time.sleep(0.05)
        assert len(peer.cancelled_req_ids) >= 1, "Expected cancel op propagation on client disconnect"

        # Cancel propagation on non-streaming client disconnect
        prev_cancels = len(peer.cancelled_req_ids)
        c_sock_ns, s_sock_ns = socket.socketpair()
        t_ns = threading.Thread(target=gw.GatewayHTTPHandler, args=(s_sock_ns, ("127.0.0.1", 0), srv_dummy), daemon=True)
        t_ns.start()
        ns_can_payload = json.dumps({"model": "gemini-app-carbon", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_CANCEL_STREAM"}]}).encode("utf-8")
        c_sock_ns.sendall(
            f"POST /v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nX-Claude-Code-Session-Id: sess-cancel-ns\r\nContent-Length: {len(ns_can_payload)}\r\n\r\n".encode("utf-8") + ns_can_payload
        )
        time.sleep(0.15)
        c_sock_ns.close()
        t_ns.join(timeout=4.0)
        try:
            s_sock_ns.close()
        except Exception:
            pass
        for _ in range(20):
            if len(peer.cancelled_req_ids) > prev_cancels:
                break
            time.sleep(0.05)
        assert len(peer.cancelled_req_ids) > prev_cancels, "Expected cancel op propagation on non-streaming client disconnect"

        # Claude CLI volatile `<system-reminder>` mutation across turns stays `sticky_delta`
        c_sr1, _, raw_sr1 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-thinking", "stream": False, "messages": [{"role": "user", "content": "<system-reminder>t=1</system-reminder>\nHello CLI"}]},
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-sr-sticky"},
        )
        assert c_sr1 == 200
        sr1_txt = "".join(b.get("text", "") for b in json.loads(raw_sr1)["content"] if b["type"] == "text")
        c_sr2, h_sr2, _ = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "<system-reminder>t=2 CHANGED</system-reminder>\nHello CLI"},
                    {"role": "assistant", "content": [{"type": "text", "text": sr1_txt}]},
                    {"role": "user", "content": "Next question"},
                ],
            },
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-sr-sticky"},
        )
        assert c_sr2 == 200 and h_sr2.get("x-turn-mode") == "sticky_delta", f"Volatile reminder broke sticky_delta: {h_sr2}"

        # Claude CLI trailing `role: "system"` (# Environment) moving from Turn 1 end to Turn 2 pre-tool_result stays `sticky_delta`
        c_ce1, _, raw_ce1 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "tools": host_tools,
                "messages": [
                    {"role": "user", "content": "Read DEMO_GUIDE.md"},
                    {"role": "system", "content": "# Environment\nInvoked in /tmp"},
                ],
            },
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-cli-trailing-sys"},
        )
        assert c_ce1 == 200
        ce1_txt = "".join(b.get("text", "") for b in json.loads(raw_ce1)["content"] if b["type"] == "text")
        c_ce2, h_ce2, _ = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "tools": host_tools,
                "messages": [
                    {"role": "user", "content": "Read DEMO_GUIDE.md"},
                    {"role": "assistant", "content": [{"type": "text", "text": ce1_txt}]},
                    {"role": "system", "content": "# Environment\nInvoked in /tmp"},
                    {"role": "user", "content": "Continue"},
                ],
            },
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-cli-trailing-sys"},
        )
        assert c_ce2 == 200 and h_ce2.get("x-turn-mode") == "sticky_delta", f"Trailing system shift broke sticky_delta: {h_ce2}"

        return {
            "zero_quota_fast_paths": "PASS (0 upstream calls)",
            "pure_layer_b_short_circuit": "PASS",
            "mixed_layer_ab_stitching": "PASS",
            "lossless_48k_pagination": "PASS",
            "tombstone_watermark_rollover": "PASS (non-thrashing verified)",
            "account_alignment_guard": "PASS (HTTP 403 pre-flight)",
            "direct_delta_early_stop": "PASS",
            "realtime_80kb_write_streaming": f"PASS ({len(json_deltas)} live JSON delta chunks, >80KB intact)",
            "malformed_envelope_self_heal": "PASS (zero leaked error text, recovered tool_use)",
            "v44_api_compliance": "PASS (400 validation, stop_sequences, thinking.disabled, tool_choice, OpenAI Chat+Responses SSE, cancel propagation)",
        }
    finally:
        gw.EMBEDDED_MCP_HOST.shutdown_all()
        gw.SOFT_WATERMARK_TOKENS = orig_wm
        gw.HARD_CEILING_TOKENS = orig_hwm
        gw.WEBKIT_PLANE = orig_plane
        gw.REGISTRY.cursor_store = orig_cstore
        gw.ACCOUNT_STORE = orig_astore
        for p in (tmp_sess, tmp_sess + ".lock", tmp_acct, tmp_acct + ".lock"):
            if os.path.exists(p):
                os.unlink(p)


def stage_torture_soak_matrix() -> dict:
    sys.path.insert(0, PROJECT_DIR)
    import gateway_poc as gw
    from webkit_plane_client import WebKitPlaneClient

    tmp_proto = os.path.join(RUNTIME_DIR, f"test_dg_{os.getpid()}.json")
    tmp_sess = os.path.join(RUNTIME_DIR, f"test_sess_torture_{os.getpid()}.json")
    tmp_acct = os.path.join(RUNTIME_DIR, f"test_acct_torture_{os.getpid()}.json")
    peer = MockWebKitStreamPeer()
    peer.warmup_fail_remaining = 2
    peer.canned_err_on_cursor_once = True
    peer.premature_eof_once = True

    with open(tmp_proto, "w", encoding="utf-8") as fp:
        json.dump({
            "bl": "boq_gemini-web-uiserver_20261115.13_p0",
            "capacity": [4, 33, 22, 5, 6, 8, 16, 19, 20, 38],
            "discovered_models": {
                "Carbon": {"hex": "95a4c0abcba24364", "label": "Carbon", "mode": 8},
                "4 Hydrogen": {"hex": "56fdd199312815e2", "label": "4 Hydrogen", "mode": 1},
                "Flash-Lite": {"hex": "1d44b34bcaa1c04d", "label": "Flash-Lite", "mode": 6},
            },
        }, fp)

    mock_plane = WebKitPlaneClient(
        socket_path=gw.WEBKIT_SOCK,
        binary_path=gw.WEBKIT_BIN,
        auto_spawn=False,
        socket_connector=peer.create_connected_socket,
        proto_profile_path=tmp_proto,
    )
    orig_plane = gw.WEBKIT_PLANE
    orig_cstore = gw.REGISTRY.cursor_store
    orig_astore = gw.ACCOUNT_STORE
    gw.WEBKIT_PLANE = mock_plane
    gw.REGISTRY.cursor_store = gw.SessionCursorStore(tmp_sess)
    gw.ACCOUNT_STORE = gw.AccountAlignmentStore(tmp_acct)
    gw.ACCOUNT_STORE.set_pinned("")
    client = SocketPairHTTPClient(gw.GatewayHTTPHandler)

    try:
        # 1. Cold-start `BRIDGE_WARMING_UP` auto-retry + Mid-stream `role: "system"` folding
        c_wu, _, raw_wu = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "SCENARIO_WARMING_UP_RETRY"},
                    {"role": "system", "content": "MID_TURN_GUARD_RULE"},
                    {"role": "user", "content": "Please continue"},
                ],
            },
            {"x-claude-code-session-id": "sess-warmup-sys"},
        )
        assert c_wu == 200 and peer.warmup_fail_remaining == 0
        assert "sys_note=True" in raw_wu
        last_req = peer.requests_log[-1]
        assert last_req["turn_17"] == [[0]] and last_req["canvas_32"] == 1 and last_req["slot_79"] == 8 and last_req["slot_80"] == 2

        # 2. Canned HTTP-200 Upstream Error (`"I seem to be encountering an error..."`) -> Automatic `cursor_self_heal` Cold Replay!
        c_ch, h_ch, raw_ch = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "SCENARIO_WARMING_UP_RETRY"},
                    {"role": "system", "content": "MID_TURN_GUARD_RULE"},
                    {"role": "user", "content": "Please continue"},
                    {"role": "assistant", "content": [{"type": "text", "text": "ok"}]},
                    {"role": "user", "content": "SCENARIO_CANNED_CURSOR_HEAL"},
                ],
            },
            {"x-claude-code-session-id": "sess-warmup-sys"},
        )
        assert c_ch == 200 and h_ch.get("x-turn-mode") == "cursor_self_heal"
        assert "I seem to be encountering an error" not in raw_ch
        assert "常规流式应答完成" in raw_ch

        # 3. Premature Socket EOF Detection (`[BRIDGE_PREMATURE_EOF]` never silently accepted as `end_turn`!)
        c_eof, _, raw_eof = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-fast", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_PREMATURE_EOF"}]},
            {"x-claude-code-session-id": "sess-premature-eof"},
        )
        assert c_eof == 503 and "BRIDGE_PREMATURE_EOF" in raw_eof

        # 3b. Carbon `FETCH_TIMEOUT` -> Retries at FULL `4 Carbon High` (`slot_79=8, slot_80=2`) WITHOUT degrading thinking tier to Medium!
        peer.carbon_timeout_once = True
        reqs_before_step = len(peer.requests_log)
        prev_quiet = os.environ.pop("DSH_QUIET", None)
        try:
            c_st, _, raw_st = client.request(
                "POST",
                "/v1/messages",
                {"model": "gemini-app-carbon", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_CARBON_TIMEOUT_STEPDOWN"}]},
                {"x-claude-code-session-id": "sess-carbon-stepdown"},
            )
        finally:
            if prev_quiet is not None:
                os.environ["DSH_QUIET"] = prev_quiet
        assert c_st == 200 and "CARBON_HIGH_PRESERVED_OK(m8w2)" in raw_st, f"Carbon degraded: {raw_st}"
        st_reqs = peer.requests_log[reqs_before_step:]
        assert len(st_reqs) == 2 and st_reqs[0]["slot_79"] == 8 and st_reqs[0]["slot_80"] == 2
        assert st_reqs[1]["slot_79"] == 8 and st_reqs[1]["slot_80"] == 2

        # 4. Pre-First-Delta Strict Downgrade Reroute (`meta: "3.5 Flash-Lite"` on `mode=8` aborts immediately & reroutes SAME request to `4 Hydrogen High` `mode=1, ThinkWire=2`!)
        reqs_before_dg = len(peer.requests_log)
        c_dg1, h_dg1, raw_dg1 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_DOWNGRADE_GUARD"}]},
            {"x-claude-code-session-id": "sess-dg-live"},
        )
        assert c_dg1 == 200
        dg_reqs = peer.requests_log[reqs_before_dg:]
        assert len(dg_reqs) == 2 and dg_reqs[0]["slot_79"] == 8 and dg_reqs[1]["slot_79"] == 1 and dg_reqs[1]["slot_80"] == 2
        assert "FLASH_LITE_DOWNGRADED_TEXT_MUST_BE_ABORTED" not in raw_dg1
        msg_dg1 = json.loads(raw_dg1)
        thk_dg1 = "".join(b.get("thinking", "") for b in msg_dg1["content"] if b["type"] == "thinking")
        txt_dg1 = "".join(b.get("text", "") for b in msg_dg1["content"] if b["type"] == "text")
        assert "4 Hydrogen High" in thk_dg1 and "无缝接管完成" in txt_dg1
        assert "+rerouted(m1w2)" in h_dg1.get("x-stream-mode", "")

        # Subsequent request goes straight to `4 Hydrogen High` `(1, 2)` without wasting a `mode=8` probe during cooldown
        c_dg2, _, _ = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "messages": [{"role": "user", "content": "Followup after cooldown"}]},
            {"x-claude-code-session-id": "sess-dg-followup"},
        )
        assert c_dg2 == 200 and peer.requests_log[-1]["slot_79"] == 1 and peer.requests_log[-1]["slot_80"] == 2

        # 5. v4.5 Resilience: `HTTP_429 [1095]` Multi-Attempt Retry + `Carbon` -> `4 Hydrogen High (1, 2)` Same-Turn Step-Down
        mock_plane.proto_cache.mark_mode_downgraded(8, cooldown_sec=0.0)
        peer.http_429_remaining = 2
        reqs_before_429 = len(peer.requests_log)
        c_429, h_429, raw_429 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_429_CARBON_STEPDOWN"}]},
            {"x-claude-code-session-id": "sess-429-stepdown"},
        )
        assert c_429 == 200 and "429_STEPDOWN_RECOVERED_OK(m1w2)" in raw_429, f"429 stepdown failed: {c_429} {raw_429}"
        reqs_429 = peer.requests_log[reqs_before_429:]
        assert len(reqs_429) == 3 and reqs_429[0]["slot_79"] == 8 and reqs_429[-1]["slot_79"] == 1 and reqs_429[-1]["slot_80"] == 2

        # 6. v4.5 Resilience: `FIRST_TOKEN_TIMEOUT` (BOQ Heartbeat Stall) Auto-Retry Recovery
        peer.first_token_timeout_once = True
        c_ftt, _, raw_ftt = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-thinking", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_FIRST_TOKEN_TIMEOUT"}]},
            {"x-claude-code-session-id": "sess-ftt-retry"},
        )
        assert c_ftt == 200 and "FIRST_TOKEN_TIMEOUT_RECOVERED_OK" in raw_ftt

        # 7. v4.5 Resilience: `BardErrorInfo [1097]` Rewinds to Prior Committed Checkpoint (`cursor_checkpoint_rewind`) Instead of Wiping Session Cold
        peer.err_1097_on_cursor_once = True
        c_ck1, _, raw_ck1 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-thinking", "stream": False, "messages": [{"role": "user", "content": "SCENARIO_CKPT_TURN1"}]},
            {"x-claude-code-session-id": "sess-ckpt-rewind"},
        )
        assert c_ck1 == 200 and "CKPT_TURN1_OK" in raw_ck1
        c_ck2, _, raw_ck2 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "SCENARIO_CKPT_TURN1"},
                    {"role": "assistant", "content": [{"type": "text", "text": "CKPT_TURN1_OK"}]},
                    {"role": "user", "content": "SCENARIO_CKPT_TURN2"},
                ],
            },
            {"x-claude-code-session-id": "sess-ckpt-rewind"},
        )
        assert c_ck2 == 200 and "CKPT_TURN2_STALE_CURSOR" in raw_ck2
        c_ck3, h_ck3, raw_ck3 = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-thinking",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "SCENARIO_CKPT_TURN1"},
                    {"role": "assistant", "content": [{"type": "text", "text": "CKPT_TURN1_OK"}]},
                    {"role": "user", "content": "SCENARIO_CKPT_TURN2"},
                    {"role": "assistant", "content": [{"type": "text", "text": "CKPT_TURN2_STALE_CURSOR"}]},
                    {"role": "user", "content": "SCENARIO_CKPT_TURN3_1097"},
                ],
            },
            {"x-claude-code-session-id": "sess-ckpt-rewind"},
        )
        assert c_ck3 == 200 and h_ck3.get("x-turn-mode") == "cursor_checkpoint_rewind", f"Expected cursor_checkpoint_rewind, got {h_ck3}"
        assert "CKPT_REWIND_OK(cursor_rid=r_ckpt_1)" in raw_ck3

        # 8. v4.6 Clean-Carbon: Sideband `<block>no</block>` Bash Classifier Isolation + Automatic Sidebar Chat Deletion
        mock_plane.proto_cache.mark_mode_downgraded(8, cooldown_sec=0.0)
        sb_tools = [
            {"name": "Bash", "description": "Bash", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}},
            {"name": "Read", "description": "Read", "input_schema": {"type": "object", "properties": {"file_path": {"type": "string"}}}},
        ]
        c_sb1, _, raw_sb1 = client.request(
            "POST",
            "/v1/messages",
            {"model": "gemini-app-carbon", "stream": False, "tools": sb_tools, "messages": [{"role": "user", "content": "Build animation"}]},
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-sideband-guard"},
        )
        assert c_sb1 == 200
        sb1_txt = "".join(b.get("text", "") for b in json.loads(raw_sb1)["content"] if b["type"] == "text")

        # Claude Code fires toolless `<block>yes/no</block>` classifier WITH THE SAME session-id!
        c_clf, h_clf, raw_clf = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "messages": [{"role": "user", "content": "Evaluate command `open demo.html`. Reply <block>yes</block> or <block>no</block>."}],
            },
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-sideband-guard"},
        )
        assert c_clf == 200 and h_clf.get("x-turn-mode") == "ephem_classifier" and "<block>no</block>" in raw_clf

        # Main session MUST remain unbroken `sticky_delta` (NOT `rollover_compact` / `rollover_prefix`!)
        c_sb2, h_sb2, _ = client.request(
            "POST",
            "/v1/messages",
            {
                "model": "gemini-app-carbon",
                "stream": False,
                "tools": sb_tools,
                "messages": [
                    {"role": "user", "content": "Build animation"},
                    {"role": "assistant", "content": [{"type": "text", "text": sb1_txt}]},
                    {"role": "user", "content": "Now open it in browser"},
                ],
            },
            {"User-Agent": "claude-cli/2.1.325", "x-claude-code-session-id": "sess-sideband-guard"},
        )
        assert c_sb2 == 200 and h_sb2.get("x-turn-mode") == "sticky_delta", f"Classifier shattered main session: {h_sb2}"

        # Wait briefly for async delete worker and verify ephemeral classifier chat `c_ephem_block_9999` was auto-deleted!
        for _ in range(20):
            if "c_ephem_block_9999" in peer.deleted_cids:
                break
            time.sleep(0.05)
        assert "c_ephem_block_9999" in peer.deleted_cids, f"Ephemeral chat not auto-deleted: {peer.deleted_cids}"

        # Verify `/_admin/clean_chats` sweeps remaining tracked gateway chats
        c_clean, _, raw_clean = client.request("POST", "/_admin/clean_chats", {})
        assert c_clean == 200 and json.loads(raw_clean).get("ok") is True

        return {
            "cold_start_warmup_auto_retry": "PASS (recovered after 2 BRIDGE_WARMING_UP polls)",
            "canned_http200_error_self_heal": "PASS (intercepted canned error & replayed cold cleanly)",
            "premature_socket_eof_guard": "PASS ([BRIDGE_PREMATURE_EOF] caught, zero silent truncation)",
            "carbon_high_preserved_on_timeout": "PASS (Carbon timeout retries at full High w=2 without degrading to Medium)",
            "pre_delta_flash_lite_to_hydrogen_high": "PASS (aborted Flash-Lite on Frame 0 & rerouted same turn to 4 Hydrogen High)",
            "http_429_1095_stepdown_recovery": "PASS (Carbon 429 [1095] -> automatic 4 Hydrogen High step-down)",
            "first_token_watchdog_recovery": "PASS (BOQ heartbeat stall FIRST_TOKEN_TIMEOUT retried cleanly)",
            "bard_1097_checkpoint_rewind": "PASS (rewound to prior committed r_ckpt_1 without cold reset)",
            "sideband_isolation_and_auto_delete": "PASS (<block> classifier isolated & auto-deleted via GzXR5e; main session sticky_delta intact)",
        }
    finally:
        gw.WEBKIT_PLANE = orig_plane
        gw.REGISTRY.cursor_store = orig_cstore
        gw.ACCOUNT_STORE = orig_astore
        for p in (tmp_proto, tmp_proto + ".lock", tmp_sess, tmp_sess + ".lock", tmp_acct, tmp_acct + ".lock"):
            if os.path.exists(p):
                os.unlink(p)


def stage_live_smoke_gate() -> dict:
    sys.path.insert(0, PROJECT_DIR)
    import gateway_poc as gw
    from dsh_mcp_stdio import fifo_rpc_request

    client = SocketPairHTTPClient(gw.GatewayHTTPHandler)
    c_h, _, raw_h = client.request("GET", "/health")
    assert c_h == 200
    health = json.loads(raw_h)
    assert health.get("ok") is True
    assert health.get("version") == TARGET_VERSION
    assert health.get("acp_spawned") is False

    c_r, _, raw_r = client.request("GET", "/readyz")
    assert c_r == 200 and json.loads(raw_r).get("version") == TARGET_VERSION

    c_m, _, raw_m = client.request("GET", "/v1/models")
    assert c_m == 200
    model_ids = [m["id"] for m in json.loads(raw_m).get("data", [])]
    assert model_ids == ["gemini-app-carbon", "gemini-app-thinking", "gemini-app-fast"]

    c_m1, _, _ = client.request("GET", "/v1/models/gemini-app-carbon")
    assert c_m1 == 200
    c_m404, _, _ = client.request("GET", "/v1/models/nonexistent-model-xyz")
    assert c_m404 == 404

    c_d, _, dash_html = client.request("GET", "/dashboard")
    assert c_d == 200 and "Dual-Plane v4.6" in dash_html

    fifo_dir = f"/tmp/dsh_smoke_fifo_{os.getpid()}"
    stop_ev = threading.Event()
    t_fifo = threading.Thread(target=gw.serve_fifo_bus, args=(fifo_dir, stop_ev), daemon=True)
    t_fifo.start()
    try:
        for _ in range(20):
            if os.path.exists(os.path.join(fifo_dir, "wake.fifo")):
                break
            time.sleep(0.05)
        fifo_res = fifo_rpc_request(fifo_dir, "GET", "/health", {})
        fifo_body = json.loads(fifo_res["body"])
        assert fifo_body.get("version") == TARGET_VERSION
    finally:
        stop_ev.set()
        t_fifo.join(timeout=2.5)
        shutil.rmtree(fifo_dir, ignore_errors=True)
        gw.EMBEDDED_MCP_HOST.shutdown_all()

    return {
        "mode": "socketpair+fifo_verified",
        "version": health["version"],
        "acp_spawned": health["acp_spawned"],
        "readyz_endpoint": "PASS",
        "webkit_binary_exists": bool((health.get("webkit_plane") or {}).get("binary_exists")),
        "embedded_local_mcp_servers": (health.get("embedded_local_mcp") or {}).get("server_count", 0),
        "plane2_fifo_bus": "PASS",
        "models_registered": model_ids,
    }


def main() -> int:
    stages: List[Tuple[str, Callable[[], dict]]] = [
        ("1. preflight_env", stage_preflight_env),
        ("2. build_native", stage_build_native),
        ("3. unit_fsm_and_mcp", stage_unit_fsm_and_mcp),
        ("4. protocol_e2e", stage_protocol_e2e),
        ("5. torture_soak_matrix", stage_torture_soak_matrix),
        ("6. live_smoke_gate", stage_live_smoke_gate),
    ]

    report: Dict[str, Any] = {
        "project": "gemini-app-api-poc",
        "target_version": TARGET_VERSION,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "stages": [],
        "overall_status": "PENDING",
    }

    t_all = time.time()
    for name, fn in stages:
        sys.stdout.write(f"[Workflow] Running {name} ...\n")
        sys.stdout.flush()
        t0 = time.time()
        try:
            details = fn()
            elapsed_ms = int((time.time() - t0) * 1000)
            report["stages"].append({"stage": name, "status": "PASS", "elapsed_ms": elapsed_ms, "details": details})
            sys.stdout.write(f"[Workflow] PASS {name} ({elapsed_ms} ms) -> {json.dumps(details, ensure_ascii=False)}\n")
            sys.stdout.flush()
        except Exception as exc:
            elapsed_ms = int((time.time() - t0) * 1000)
            report["stages"].append({"stage": name, "status": "FAIL", "elapsed_ms": elapsed_ms, "error": str(exc)})
            report["overall_status"] = "FAIL"
            report["total_elapsed_ms"] = int((time.time() - t_all) * 1000)
            with open(REPORT_PATH, "w", encoding="utf-8") as fp:
                json.dump(report, fp, indent=2, ensure_ascii=False)
            sys.stderr.write(f"[Workflow] FAIL {name}: {exc}\n")
            return 1

    report["overall_status"] = "PASS"
    report["total_elapsed_ms"] = int((time.time() - t_all) * 1000)
    with open(REPORT_PATH, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, ensure_ascii=False)
    sys.stdout.write(f"\n[Workflow] ALL 6 STAGES PASSED in {report['total_elapsed_ms']} ms! Report saved to {REPORT_PATH}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
