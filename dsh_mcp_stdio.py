#!/usr/bin/env python3
"""
dsh_mcp_stdio.py v1.4 - Stdio MCP bridge for session-scoped direct tools (`DSH_SESSION_TOKEN`)
and optional manual worker fallback.
"""

import errno
import http.client
import json
import os
import select
import sys
import threading
import time
import uuid
from typing import Optional

DEFAULT_IPC_DIR = os.environ.get("DSH_IPC_DIR", "/tmp/dsh_gemini_ipc_18896")
DEFAULT_HTTP_HOST = os.environ.get("DSH_GATEWAY_HOST", "127.0.0.1")
DEFAULT_HTTP_PORT = int(os.environ.get("DSH_GATEWAY_PORT", "18896"))
SESSION_TOKEN = os.environ.get("DSH_SESSION_TOKEN", "").strip()
MAX_LOCAL_WAIT_SEC = float(os.environ.get("DSH_LOCAL_WAIT_SEC", "160.0"))
DEFAULT_FIFO_RPC_TIMEOUT = float(os.environ.get("DSH_FIFO_RPC_TIMEOUT", "30.0"))


class FifoUnavailableError(OSError):
    """Raised when no gateway process has the IPC wake FIFO open."""


def read_fifo_with_timeout(fifo_path: str, timeout_sec: float) -> str:
    fd = os.open(fifo_path, os.O_RDONLY | os.O_NONBLOCK)
    chunks = []
    writer_seen = False
    deadline = time.time() + max(0.1, float(timeout_sec))
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


def _wake_gateway_fifo(wake_fifo: str) -> None:
    try:
        fd = os.open(wake_fifo, os.O_WRONLY | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ENXIO:
            try:
                os.unlink(wake_fifo)
            except OSError:
                pass
            raise FifoUnavailableError(errno.ENXIO, f"No gateway listening on stale FIFO {wake_fifo}") from exc
        if exc.errno == errno.ENOENT:
            raise FifoUnavailableError(errno.ENOENT, f"Wake FIFO not found: {wake_fifo}") from exc
        raise
    try:
        os.write(fd, b"1")
    except OSError as exc:
        if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
            raise
    finally:
        try:
            os.close(fd)
        except OSError:
            pass

stdout_lock = threading.Lock()
SAFE_RELAY_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": False,
    "openWorldHint": False,
}


def send_stdout(obj: dict) -> None:
    line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    with stdout_lock:
        sys.stdout.write(line)
        sys.stdout.flush()


def fifo_rpc_request(
    ipc_dir: str,
    method: str,
    path: str,
    body_obj: Optional[dict],
    headers: Optional[dict] = None,
    timeout_sec: Optional[float] = None,
) -> dict:
    req_id = uuid.uuid4().hex
    resp_fifo = os.path.join(ipc_dir, f"resp_{req_id}.fifo")
    req_tmp = os.path.join(ipc_dir, f"req_{req_id}.tmp")
    req_final = os.path.join(ipc_dir, f"req_{req_id}.json")
    wake_fifo = os.path.join(ipc_dir, "wake.fifo")
    rpc_timeout = (
        float(timeout_sec)
        if timeout_sec is not None
        else float(os.environ.get("DSH_FIFO_RPC_TIMEOUT", DEFAULT_FIFO_RPC_TIMEOUT))
    )

    os.mkfifo(resp_fifo, 0o600)
    try:
        envelope = {
            "id": req_id,
            "method": method,
            "path": path,
            "headers": dict(headers or {}),
            "body": body_obj,
            "resp_fifo": resp_fifo,
        }
        with open(req_tmp, "w", encoding="utf-8") as fp:
            json.dump(envelope, fp, ensure_ascii=False)
        os.replace(req_tmp, req_final)

        _wake_gateway_fifo(wake_fifo)

        raw_resp = read_fifo_with_timeout(resp_fifo, timeout_sec=rpc_timeout)
        return json.loads(raw_resp)
    finally:
        for p in (resp_fifo, req_tmp, req_final):
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


def single_gateway_rpc(payload: dict) -> dict:
    if SESSION_TOKEN and isinstance(payload.get("params"), dict):
        payload["params"].setdefault("session_token", SESSION_TOKEN)
        if isinstance(payload["params"].get("arguments"), dict):
            payload["params"]["arguments"].setdefault("session_token", SESSION_TOKEN)

    ipc_dir = os.environ.get("DSH_IPC_DIR", DEFAULT_IPC_DIR)
    if os.path.isdir(ipc_dir) and os.path.exists(os.path.join(ipc_dir, "wake.fifo")):
        tname = ((payload.get("params") or {}).get("name")) or ""
        rpc_timeout: Optional[float] = None
        if (
            payload.get("method") == "tools/call"
            and tname not in ("dsh_wait_request", "dsh_await_request")
            and "DSH_FIFO_RPC_TIMEOUT" not in os.environ
        ):
            rpc_timeout = max(DEFAULT_FIFO_RPC_TIMEOUT, 65.0)
        try:
            env_resp = fifo_rpc_request(ipc_dir, "POST", "/mcp/shared", payload, timeout_sec=rpc_timeout)
            return json.loads(env_resp["body"])
        except FifoUnavailableError:
            pass
        except OSError as exc:
            if exc.errno not in (errno.ENXIO, errno.ENOENT):
                return {
                    "jsonrpc": "2.0",
                    "id": payload.get("id"),
                    "result": {"content": [{"type": "text", "text": f"[DSH_TRANSIENT_IPC_ERR] {exc}"}], "isError": True},
                }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "result": {"content": [{"type": "text", "text": f"[DSH_TRANSIENT_IPC_ERR] {exc}"}], "isError": True},
            }

    conn = None
    try:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        conn = http.client.HTTPConnection(DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, timeout=300.0)
        conn.request("POST", "/mcp/shared", body=body, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception as exc:
        return {
            "jsonrpc": "2.0",
            "id": payload.get("id"),
            "result": {"content": [{"type": "text", "text": f"[DSH_TRANSIENT_IPC_ERR] {exc}"}], "isError": True},
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def forward_to_gateway(payload: dict) -> dict:
    params = payload.get("params") or {}
    tname = params.get("name", "")
    if tname not in ("dsh_wait_request", "dsh_await_request"):
        return single_gateway_rpc(payload)

    deadline = time.time() + MAX_LOCAL_WAIT_SEC
    last_resp = None
    while True:
        resp = single_gateway_rpc(payload)
        last_resp = resp
        txt = (((resp.get("result") or {}).get("content") or [{}])[0]).get("text", "")
        if "[DSH_TRANSIENT_IPC_ERR]" in txt:
            if time.time() + 1.0 >= deadline:
                return resp
            time.sleep(0.8)
            continue
        if "[DSH_IDLE_HEARTBEAT" in txt and (time.time() + 22.0 < deadline):
            continue
        return last_resp


def handle_message(msg: dict) -> None:
    msg_id = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params") or {}

    if msg_id is None:
        return

    if method == "initialize":
        proto = params.get("protocolVersion") or "2025-06-18"
        send_stdout({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": proto,
                "capabilities": {"logging": {}, "tools": {"listChanged": False}},
                "serverInfo": {"name": "dsh-bridge", "version": "1.4.0"},
            },
        })
        return

    if method == "ping":
        send_stdout({"jsonrpc": "2.0", "id": msg_id, "result": {}})
        return

    if method == "tools/list":
        if SESSION_TOKEN:
            resp = single_gateway_rpc({"jsonrpc": "2.0", "id": msg_id, "method": "tools/list", "params": {"session_token": SESSION_TOKEN}})
            resp["id"] = msg_id
            resp["jsonrpc"] = "2.0"
            send_stdout(resp)
            return
        send_stdout({"jsonrpc": "2.0", "id": msg_id, "result": {"tools": []}})
        return

    if method == "resources/list":
        send_stdout({"jsonrpc": "2.0", "id": msg_id, "result": {"resources": []}})
        return

    if method == "resources/templates/list":
        send_stdout({"jsonrpc": "2.0", "id": msg_id, "result": {"resourceTemplates": []}})
        return

    if method == "prompts/list":
        send_stdout({"jsonrpc": "2.0", "id": msg_id, "result": {"prompts": []}})
        return

    if method == "tools/call":
        resp = forward_to_gateway(msg)
        resp["id"] = msg_id
        resp["jsonrpc"] = "2.0"
        send_stdout(resp)
        return

    send_stdout({
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    })


def main() -> None:
    workers = []
    while True:
        raw_line = sys.stdin.readline()
        if not raw_line:
            break
        line = raw_line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception as exc:
            send_stdout({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"Parse error: {exc}"}})
            continue
        if not isinstance(msg, dict):
            send_stdout({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}})
            continue
        workers = [w for w in workers if w.is_alive()]
        if msg.get("method") == "tools/call":
            t = threading.Thread(target=handle_message, args=(msg,), daemon=True)
            t.start()
            workers.append(t)
        else:
            handle_message(msg)

    for t in [w for w in workers if w.is_alive()]:
        t.join(timeout=2.0)


if __name__ == "__main__":
    main()
