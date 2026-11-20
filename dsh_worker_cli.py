#!/usr/bin/env python3
"""
dsh_worker_cli.py v2.0 - Non-blocking Zero-MCP POSIX FIFO Worker CLI for Gemini.app Web Quota Mode.
Communicates with gateway_poc.py purely via /tmp/dsh_gemini_ipc_18896 POSIX FIFOs:
  - 100% Gemini.app Web Quota (ZERO `agy acp`, ZERO MCP config, ZERO approval popups with RequiresNetworkAccess=false)
  - Non-blocking `O_RDONLY | O_NONBLOCK` + `select` FIFO reader: survives gateway restarts & pre-boot startup!
  - Clean `[DSH_IDLE_TIMEOUT]` exit code 0 after `--timeout` seconds for continuous background worker loops.
"""

import argparse
import errno
import json
import os
import re
import select
import sys
import time
import uuid

DEFAULT_PORT = os.environ.get("DSH_GATEWAY_PORT", "18896")
DEFAULT_IPC_DIR = os.environ.get("DSH_IPC_DIR", f"/tmp/dsh_gemini_ipc_{DEFAULT_PORT}")


def read_fifo_with_timeout(fifo_path: str, timeout_sec: float) -> str:
    fd = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
    chunks = []
    writer_seen = False
    deadline = time.time() + timeout_sec
    try:
        while True:
            rem = deadline - time.time()
            if rem <= 0:
                raise TimeoutError(f"Timed out waiting {timeout_sec:.0f}s for gateway FIFO response")
            r, _, _ = select.select([fd], [], [], min(0.5, rem))
            if not r:
                continue
            try:
                data = os.read(fd, 65536)
            except OSError as exc:
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    continue
                raise
            if data:
                writer_seen = True
                chunks.append(data)
            else:
                if writer_seen:
                    break
                # On macOS, O_NONBLOCK FIFO returns b"" immediately before any writer opens it
                time.sleep(0.04)
        return b"".join(chunks).decode("utf-8", errors="replace")
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def fifo_rpc_once(ipc_dir: str, payload: dict, rpc_timeout: float = 28.0) -> dict:
    wake_fifo = os.path.join(ipc_dir, "wake.fifo")
    if not os.path.exists(wake_fifo):
        return {"isError": True, "text": f"[DSH_TRANSIENT_IPC_ERR] Gateway FIFO not ready at {ipc_dir}"}

    req_id = uuid.uuid4().hex
    resp_fifo = os.path.join(ipc_dir, f"resp_{req_id}.fifo")
    req_tmp = os.path.join(ipc_dir, f"req_{req_id}.tmp")
    req_final = os.path.join(ipc_dir, f"req_{req_id}.json")

    try:
        os.mkfifo(resp_fifo, 0o600)
    except OSError as exc:
        return {"isError": True, "text": f"[DSH_TRANSIENT_IPC_ERR] mkfifo failed: {exc}"}

    try:
        envelope = {
            "id": req_id,
            "method": "POST",
            "path": "/mcp/shared",
            "headers": {},
            "body": payload,
            "resp_fifo": resp_fifo,
        }
        with open(req_tmp, "w", encoding="utf-8") as fp:
            json.dump(envelope, fp, ensure_ascii=False)
        os.replace(req_tmp, req_final)

        try:
            wfd = os.open(wake_fifo, os.O_WRONLY | os.O_NONBLOCK)
            os.write(wfd, b"1")
            os.close(wfd)
        except OSError:
            pass

        raw = read_fifo_with_timeout(resp_fifo, timeout_sec=rpc_timeout)
        outer = json.loads(raw)
        rpc_res = json.loads(outer["body"]).get("result") or {}
        txt = ((rpc_res.get("content") or [{}])[0]).get("text", "")
        return {"isError": bool(rpc_res.get("isError", False)), "text": txt}
    except Exception as exc:
        return {"isError": True, "text": f"[DSH_TRANSIENT_IPC_ERR] {exc}"}
    finally:
        for p in (resp_fifo, req_tmp, req_final):
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


def call_meta_tool(ipc_dir: str, name: str, arguments: dict, rpc_timeout: float = 28.0) -> dict:
    return fifo_rpc_once(
        ipc_dir,
        {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().hex[:8],
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
        rpc_timeout=rpc_timeout,
    )


def wait_loop(ipc_dir: str, worker_id: str, worker_model: str, timeout_sec: float) -> int:
    deadline = time.time() + max(5.0, timeout_sec)
    wid = worker_id or f"web_{worker_model.split('-')[-1]}_{os.getpid()}"
    while True:
        rem = deadline - time.time()
        if rem <= 0:
            sys.stdout.write(f"[DSH_IDLE_TIMEOUT worker_id={wid}] No request arrived within {timeout_sec:.0f}s; run `wait` again to stay on standby.\n")
            sys.stdout.flush()
            return 0
        res = call_meta_tool(ipc_dir, "dsh_wait_request", {"worker_id": wid, "worker_model": worker_model}, rpc_timeout=min(26.0, max(4.0, rem + 3.0)))
        txt = res["text"]
        if "[DSH_TRANSIENT_IPC_ERR]" in txt:
            if time.time() + 0.8 >= deadline:
                sys.stdout.write(f"[DSH_IDLE_TIMEOUT worker_id={wid}] Gateway not started yet ({txt}); run `wait` again to stay on standby.\n")
                sys.stdout.flush()
                return 0
            time.sleep(0.8)
            continue
        if "[DSH_IDLE_HEARTBEAT" in txt:
            if time.time() + 1.0 >= deadline:
                sys.stdout.write(f"[DSH_IDLE_TIMEOUT worker_id={wid}] Heartbeat cycle complete; run `wait` again to stay on standby.\n")
                sys.stdout.flush()
                return 0
            continue
        sys.stdout.write(txt + "\n")
        sys.stdout.flush()
        return 1 if res["isError"] else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Zero-MCP Gemini.app Web Quota Worker CLI v2.0")
    ap.add_argument("--ipc-dir", default=DEFAULT_IPC_DIR)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_wait = sub.add_parser("wait")
    p_wait.add_argument("--worker-id", default="")
    p_wait.add_argument("--model", default="gemini-app-carbon")
    p_wait.add_argument("--timeout", type=float, default=600.0)

    p_daemon = sub.add_parser("daemon")
    p_daemon.add_argument("--worker-id", default="")
    p_daemon.add_argument("--model", default="gemini-app-carbon")
    p_daemon.add_argument("--max-idle", type=float, default=1800.0)

    p_call = sub.add_parser("call")
    p_call.add_argument("--token", required=True)
    p_call.add_argument("--tool", required=True)
    p_call.add_argument("--args", default="{}")
    p_call.add_argument("--args-file", default="")

    p_page = sub.add_parser("page")
    p_page.add_argument("--token", required=True)
    p_page.add_argument("--page-token", required=True)
    p_page.add_argument("--page", type=int, required=True)

    p_stream = sub.add_parser("stream")
    p_stream.add_argument("--token", required=True)
    p_stream.add_argument("--thinking", default="")
    p_stream.add_argument("--thinking-file", default="")
    p_stream.add_argument("--delta", default="")
    p_stream.add_argument("--delta-file", default="")

    p_reply = sub.add_parser("reply")
    p_reply.add_argument("--token", required=True)
    p_reply.add_argument("--thinking", default="")
    p_reply.add_argument("--content", default="")
    p_reply.add_argument("--content-file", default="")
    p_reply.add_argument("--wait-next", action="store_true")
    p_reply.add_argument("--model", default="gemini-app-carbon")
    p_reply.add_argument("--timeout", type=float, default=250.0)

    ns = ap.parse_args()

    if ns.cmd == "wait":
        return wait_loop(ns.ipc_dir, ns.worker_id, ns.model, ns.timeout)

    if ns.cmd == "daemon":
        return wait_loop(ns.ipc_dir, ns.worker_id, ns.model, ns.max_idle)

    if ns.cmd == "call":
        raw_args = open(ns.args_file, "r", encoding="utf-8").read() if ns.args_file else ns.args
        parsed_args = json.loads(raw_args) if raw_args.strip() else {}
        res = call_meta_tool(
            ns.ipc_dir,
            "dsh_call_tool",
            {"session_token": ns.token, "name": ns.tool, "arguments": parsed_args},
            rpc_timeout=65.0,
        )
        while res["text"].startswith("Tool call ") and "is still running in the host harness" in res["text"]:
            m = re.search(r'"call_id"\s*:\s*"([^"]+)"', res["text"])
            if not m:
                break
            res = call_meta_tool(
                ns.ipc_dir,
                "dsh_await_result",
                {"session_token": ns.token, "call_id": m.group(1)},
                rpc_timeout=65.0,
            )
        sys.stdout.write(res["text"] + "\n")
        sys.stdout.flush()
        return 1 if res["isError"] else 0

    if ns.cmd == "page":
        res = call_meta_tool(
            ns.ipc_dir,
            "dsh_read_tool_page",
            {"session_token": ns.token, "page_token": ns.page_token, "page": ns.page},
            rpc_timeout=20.0,
        )
        sys.stdout.write(res["text"] + "\n")
        sys.stdout.flush()
        return 1 if res["isError"] else 0

    if ns.cmd == "stream":
        thk = open(ns.thinking_file, "r", encoding="utf-8").read() if ns.thinking_file else ns.thinking
        dlt = open(ns.delta_file, "r", encoding="utf-8").read() if ns.delta_file else ns.delta
        res = call_meta_tool(
            ns.ipc_dir,
            "dsh_stream_chunk",
            {"session_token": ns.token, "thinking": thk, "delta": dlt},
            rpc_timeout=15.0,
        )
        sys.stdout.write(res["text"] + "\n")
        sys.stdout.flush()
        return 1 if res["isError"] else 0

    if ns.cmd == "reply":
        body_txt = open(ns.content_file, "r", encoding="utf-8").read() if ns.content_file else ns.content
        res = call_meta_tool(
            ns.ipc_dir,
            "dsh_reply",
            {"session_token": ns.token, "thinking": ns.thinking, "content": body_txt, "wait_next": bool(ns.wait_next)},
            rpc_timeout=26.0,
        )
        if ns.wait_next and ("Session idle" in res["text"] or "soft watermark" in res["text"] or "[DSH_TRANSIENT_IPC_ERR]" in res["text"]):
            return wait_loop(ns.ipc_dir, "", ns.model, ns.timeout)
        sys.stdout.write(res["text"] + "\n")
        sys.stdout.flush()
        return 1 if res["isError"] else 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
