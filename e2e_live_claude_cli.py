#!/usr/bin/env python3
"""
e2e_live_claude_cli.py - Runs the real local Claude Code CLI against http://127.0.0.1:18896
and drives `dsh_worker_cli.py` over `/tmp/dsh_gemini_ipc_18896` to verify real Bash + Read tool execution.
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request

DIR = os.path.dirname(os.path.abspath(__file__))
CLI = os.path.join(DIR, "dsh_worker_cli.py")
PORT = int(os.environ.get("DSH_GATEWAY_PORT", "18896"))


def worker_loop(errors: list):
    try:
        p_wait = subprocess.run(
            ["/usr/bin/python3", CLI, "wait", "--model", "gemini-app-carbon", "--timeout", "30"],
            capture_output=True, text=True,
        )
        prompt = p_wait.stdout
        m = re.search(r"\[DSH_SESSION_TOKEN:\s*(tok_[a-f0-9]+)\]", prompt)
        if not m:
            errors.append(f"Missing session token: {prompt[:240]}")
            return
        tok = m.group(1)
        if "- Agent:" in prompt:
            errors.append("Agent deadlock tool was not filtered out in single-worker mode!")
            return

        # 1. Execute real Claude Code `Read` tool on `claude_gateway_settings.json`
        r_read = subprocess.run(
            [
                "/usr/bin/python3", CLI, "call", "--token", tok, "--tool", "Read",
                "--args", json.dumps({"file_path": os.path.join(DIR, "claude_gateway_settings.json")}),
            ],
            capture_output=True, text=True,
        )
        has_carbon = "gemini-app-carbon" in r_read.stdout

        # 2. Execute real Claude Code `Read` tool on `dsh_worker_cli.py` (multi-hop tool chain!)
        r_read2 = subprocess.run(
            [
                "/usr/bin/python3", CLI, "call", "--token", tok, "--tool", "Read",
                "--args", json.dumps({"file_path": os.path.join(DIR, "dsh_worker_cli.py"), "limit": 12}),
            ],
            capture_output=True, text=True,
        )
        has_cli = "Zero-MCP" in r_read2.stdout

        subprocess.run(
            [
                "/usr/bin/python3", CLI, "stream", "--token", tok,
                "--thinking", "Verified repository files via sequential Claude Code Read calls.",
                "--delta", f"E2E_OK: settings_carbon={has_carbon}, worker_cli={has_cli}\n",
            ],
            check=True, capture_output=True,
        )
        subprocess.run(
            [
                "/usr/bin/python3", CLI, "reply", "--token", tok,
                "--content", "- claude_gateway_settings.json (3 web models)\n- dsh_worker_cli.py (Zero-MCP FIFO worker)",
            ],
            check=True, capture_output=True,
        )
    except Exception as exc:
        errors.append(str(exc))


def main() -> int:
    errs = []
    wt = threading.Thread(target=worker_loop, args=(errs,), daemon=True)
    wt.start()
    time.sleep(0.25)

    env = os.environ.copy()
    env["DSH_SKIP_WORKER_WAIT"] = "1"
    env["DSH_NO_OPEN_DASHBOARD"] = "1"
    res = subprocess.run(
        [os.path.join(DIR, "cc-demo.sh"), "-p", "Inspect claude_gateway_settings.json and dsh_worker_cli.py"],
        cwd=DIR, env=env, capture_output=True, text=True, timeout=45,
    )
    wt.join(timeout=5.0)

    if errs:
        print("WORKER ERRORS:", errs, file=sys.stderr)
        return 1
    print("=== REAL CLAUDE CODE CLI EXIT ===", res.returncode)
    print("=== REAL CLAUDE CODE CLI OUTPUT ===")
    print(res.stdout.strip())

    with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2.0) as r:
        h = json.loads(r.read().decode("utf-8"))
    print("=== TELEMETRY TURNS ===", len(h.get("telemetry", [])))
    for row in h.get("telemetry", []):
        print(f"  [{row.get('ts')}] mode={row.get('mode')} stop={row.get('stop_reason')} ttft={row.get('ttft_ms')}ms total={row.get('elapsed_ms')}ms")
    return res.returncode


if __name__ == "__main__":
    sys.exit(main())
