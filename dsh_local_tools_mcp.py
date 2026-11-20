#!/usr/bin/env python3
"""
dsh_local_tools_mcp.py - Zero-Public-IP Local Stdio MCP Server (Layer B Built-In Provider)
Runs strictly as a local child process of `gateway_poc.py` over `stdio` JSON-RPC 2.0,
requiring ZERO public IP, ZERO port forwarding, and ZERO tunnels even when LLM inference
runs on pure `gemini.google.com` Web quota.
"""

import json
import os
import platform
import sys
import time


def send_jsonrpc(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


TOOLS_MANIFEST = [
    {
        "name": "dsh_local_mcp_probe",
        "description": "Zero-public-IP local stdio MCP diagnostic probe returning host OS, PID, CWD, and local loopback status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "echo": {
                    "type": "string",
                    "description": "Optional verification string to echo from the local stdio MCP process.",
                }
            },
        },
    }
]


def handle_rpc(msg: dict) -> None:
    if not isinstance(msg, dict):
        return
    msg_id = msg.get("id")
    method = msg.get("method", "")
    raw_params = msg.get("params")
    params = raw_params if isinstance(raw_params, dict) else {}

    if msg_id is None:
        return

    if method == "initialize":
        proto = params.get("protocolVersion") or "2025-06-18"
        send_jsonrpc({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": proto,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "dsh-local-mcp", "version": "4.0.0"},
            },
        })
        return

    if method == "ping":
        send_jsonrpc({"jsonrpc": "2.0", "id": msg_id, "result": {}})
        return

    if method == "tools/list":
        send_jsonrpc({"jsonrpc": "2.0", "id": msg_id, "result": {"tools": TOOLS_MANIFEST}})
        return

    if method == "tools/call":
        tname = params.get("name", "")
        raw_args = params.get("arguments")
        if isinstance(raw_args, str):
            try:
                decoded = json.loads(raw_args)
                raw_args = decoded if isinstance(decoded, dict) else {}
            except Exception:
                raw_args = {}
        args = raw_args if isinstance(raw_args, dict) else {}
        if tname == "dsh_local_mcp_probe":
            payload = {
                "ok": True,
                "layer": "Layer-B EmbeddedLocalMcpHost (stdio, zero public IP)",
                "pid": os.getpid(),
                "platform": platform.platform(),
                "cwd": os.getcwd(),
                "echo": str(args.get("echo", "verified")),
                "ts": time.time(),
            }
            send_jsonrpc({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}],
                    "isError": False,
                },
            })
            return
        send_jsonrpc({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "content": [{"type": "text", "text": f"Unknown local MCP tool: {tname}"}],
                "isError": True,
            },
        })
        return

    send_jsonrpc({
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    })


def main() -> None:
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        s = line.strip()
        if not s:
            continue
        try:
            msg = json.loads(s)
        except Exception as exc:
            send_jsonrpc({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(exc)}})
            continue
        if not isinstance(msg, dict):
            send_jsonrpc({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request: expected JSON object"}})
            continue
        handle_rpc(msg)


if __name__ == "__main__":
    main()
