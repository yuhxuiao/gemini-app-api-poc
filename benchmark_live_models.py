#!/usr/bin/env python3
"""
[LEGACY NOTE: Early v1 PoC benchmark targeting deprecated 6-model catalog and removed ACP upstream (superseded in v4.4.0-api-ready; Dual-Plane v4.4 spawns zero ACP).]

benchmark_live_models.py - Real Cloud Model TTFT, Decode TPS, Streaming & Tool-Call
Benchmark Runner across all 6 Gateway Models (`gemini-4-carbon`, `gemini-4-argon`,
`gemini-4-hydrogen`, `gemini-3.8-flash`, `claude-opus-5-5`, `claude-sonnet-5-5`).

Usage (in macOS Terminal):
  1. First authenticate `agy` once if testing ACP channel:
       /Applications/Gemini.app/Contents/Helpers/agy acp --login
     OR start `gateway_poc.py` in `mcp_task` mode with `Gemini.app` Local MCP imported.
  2. Run:
       python3 ./benchmark_live_models.py --port 18895 --upstream acp
"""

import argparse
import http.client
import json
import time

MODELS_TO_BENCH = [
    ("gemini-3.8-flash-low", "Flash (Low)"),
    ("gemini-4-hydrogen-medium", "Hydrogen (Med)"),
    ("gemini-4-carbon-low", "Carbon (Low)"),
    ("gemini-4-carbon-high", "Carbon (High)"),
    ("gemini-4-argon-medium", "Argon (Med)"),
    ("claude-sonnet-5-5-medium", "Sonnet 5.5 (Med)"),
    ("claude-opus-5-5-high", "Opus 5.5 (High)"),
]


def bench_one_stream(host: str, port: int, model_id: str, upstream: str) -> dict:
    body = {
        "model": model_id,
        "max_tokens": 512,
        "stream": True,
        "messages": [{"role": "user", "content": "Write 3 concise bullet points explaining mutex lock ordering in Go."}],
    }
    t0 = time.perf_counter()
    conn = http.client.HTTPConnection(host, port, timeout=120.0)
    conn.request(
        "POST", "/v1/messages",
        body=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "X-Upstream-Mode": upstream,
            "X-Session-Id": f"bench_{model_id}_{int(t0)}",
        },
    )
    resp = conn.getresponse()
    first_think_ms = None
    first_text_ms = None
    think_chunks = 0
    text_chunks = 0
    out_chars = 0
    err_msg = ""

    buf = ""
    while True:
        raw = resp.fp.read1(4096) if hasattr(resp.fp, "read1") else resp.fp.read(256)
        if not raw:
            break
        now_ms = (time.perf_counter() - t0) * 1000.0
        buf += raw.decode("utf-8", errors="replace")
        while "\n\n" in buf:
            frame, buf = buf.split("\n\n", 1)
            dline = next((l[6:] for l in frame.splitlines() if l.startswith("data: ")), None)
            if not dline:
                continue
            ev = json.loads(dline)
            if ev.get("type") == "error":
                err_msg = (ev.get("error") or {}).get("message", "error")
            elif ev.get("type") == "content_block_delta":
                delta = ev["delta"]
                dt = delta.get("type")
                if dt == "thinking_delta":
                    if first_think_ms is None:
                        first_think_ms = now_ms
                    think_chunks += 1
                    out_chars += len(delta.get("thinking", ""))
                elif dt == "text_delta":
                    if first_text_ms is None:
                        first_text_ms = now_ms
                    text_chunks += 1
                    out_chars += len(delta.get("text", ""))

    total_ms = (time.perf_counter() - t0) * 1000.0
    conn.close()
    ttft = first_think_ms or first_text_ms or total_ms
    stream_ms = max(0.0, total_ms - ttft)
    est_tok = max(1, out_chars // 4)
    tps = round(est_tok / (total_ms / 1000.0), 1) if total_ms > 0 else 0.0
    return {
        "model": model_id,
        "ok": not bool(err_msg),
        "error": err_msg,
        "ttft_ms": round(ttft),
        "first_text_ms": round(first_text_ms or total_ms),
        "total_ms": round(total_ms),
        "stream_ms": round(stream_ms),
        "think_chunks": think_chunks,
        "text_chunks": text_chunks,
        "true_streaming": (think_chunks + text_chunks) > 2,
        "est_tps": tps,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18895)
    ap.add_argument("--upstream", default="acp", choices=["acp", "mcp_task"])
    args = ap.parse_args()

    print(f"| Model | Status | TTFT (1st Chunk) | 1st Text | Total | Chunks (Think/Text) | True Stream | Est TPS |")
    print(f"| :--- | :--- | ---: | ---: | ---: | ---: | :---: | ---: |")
    for mid, label in MODELS_TO_BENCH:
        r = bench_one_stream(args.host, args.port, mid, args.upstream)
        if not r["ok"]:
            print(f"| {label} (`{mid}`) | ERR: {r['error'][:42]} | - | - | {r['total_ms']}ms | 0/0 | - | - |")
        else:
            ts_flag = "YES" if r["true_streaming"] else "BUFFERED"
            print(
                f"| {label} (`{mid}`) | OK | {r['ttft_ms']}ms | {r['first_text_ms']}ms | "
                f"{r['total_ms']}ms | {r['think_chunks']}/{r['text_chunks']} | {ts_flag} | {r['est_tps']} tok/s |"
            )


if __name__ == "__main__":
    main()
