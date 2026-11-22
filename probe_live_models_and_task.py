#!/usr/bin/env python3
"""
probe_live_models_and_task.py - Full raw otAQ7b model, policy, WIZ Task/Agent flag & live model smoke prober.
Usage:
  ./cc-demo.sh probe             # Dump full raw otAQ7b, hex models, policy hdr[0..N], WIZ Task/Agent hints
  ./cc-demo.sh probe --smoke     # Also smoke-test every native model across low/medium/high via 127.0.0.1:18896
"""

import argparse
import json
import os
import re
import socket
import sys
import time
import urllib.request
from typing import Any, Dict, List

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PORT = int(os.environ.get("DSH_GATEWAY_PORT", "18896"))
DEFAULT_SOCK = os.environ.get("DSH_WEBKIT_SOCK", f"/tmp/dsh_webkit_{DEFAULT_PORT}.sock")
OUT_PATH = os.path.join(SCRIPT_DIR, "runtime", "live_probe_report.json")
HEX16_RE = re.compile(r"^[0-9a-fA-F]{16}$")


def unix_rpc(sock_path: str, payload: dict, timeout: float = 20.0) -> dict:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(sock_path)
        s.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
        buf = bytearray()
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf.extend(chunk)
        line = bytes(buf).split(b"\n", 1)[0].strip()
        return json.loads(line.decode("utf-8", errors="replace")) if line else {}
    finally:
        s.close()


def extract_hex_nodes(node: Any, path: str = "root", out: List[dict] = None) -> List[dict]:
    if out is None:
        out = []
    if isinstance(node, list):
        if node and isinstance(node[0], str) and HEX16_RE.match(node[0]):
            out.append({"path": path, "hex": node[0], "node": node[:24]})
        for i, item in enumerate(node):
            extract_hex_nodes(item, f"{path}[{i}]", out)
    elif isinstance(node, dict):
        for k, v in node.items():
            extract_hex_nodes(v, f"{path}.{k}", out)
    return out


def smoke_test_models(port: int, models: List[str], efforts: List[str]) -> List[dict]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    results: List[dict] = []
    for m in models:
        for eff in efforts:
            t0 = time.monotonic()
            body = {
                "model": m,
                "max_tokens": 256,
                "stream": False,
                "output_config": {"effort": eff},
                "messages": [{"role": "user", "content": "Reply with ONLY the integer result of 17*23."}],
            }
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/v1/messages",
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json", "x-api-key": "dsh-local-gateway"},
                method="POST",
            )
            row: Dict[str, Any] = {"model": m, "effort": eff}
            try:
                with opener.open(req, timeout=120.0) as r:
                    raw = json.loads(r.read().decode("utf-8"))
                    thk = "".join(b.get("thinking", "") for b in raw.get("content", []) if b.get("type") == "thinking")
                    txt = "".join(b.get("text", "") for b in raw.get("content", []) if b.get("type") == "text")
                    row.update({
                        "status": r.status,
                        "routed_model": r.headers.get("x-routed-model", ""),
                        "stream_mode": r.headers.get("x-stream-mode", ""),
                        "think_chars": len(thk),
                        "text_preview": txt.strip()[:80],
                        "elapsed_ms": int((time.monotonic() - t0) * 1000),
                    })
            except Exception as exc:
                row.update({
                    "status": getattr(exc, "code", 0),
                    "error": str(exc)[:240],
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                })
            results.append(row)
            print(f"[SMOKE] {m:22s} effort={eff:6s} -> status={row['status']} routed={row.get('routed_model','')} think={row.get('think_chars',0)}c {row['elapsed_ms']}ms")
    return results


def probe_neon_raw_frames(sock_path: str) -> dict:
    """Sends one reasoning prompt to gemini-4-neon with emitRawChunks=true to inspect thought/model slots."""
    from webkit_plane_client import PROTO_PROFILE_CACHE, build_stream_generate_freq

    prof = PROTO_PROFILE_CACHE.load()
    freq = build_stream_generate_freq(
        delta_prompt="Find all prime numbers p such that p^2 + 2 is also prime. Show brief reasoning then final answer.",
        cursor=("", "", ""),
        turn_index=0,
        model_id="gemini-4-neon",
        tier="high",
        prof_snapshot=prof,
    )
    extra_h = PROTO_PROFILE_CACHE.resolve_extra_headers("gemini-4-neon", tier="high", prof_snapshot=prof)
    req_id = f"neonraw_{int(time.time())}"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(120.0)
    raw_chunks: List[str] = []
    cid_seen = ""
    try:
        s.connect(sock_path)
        payload = {
            "op": "stream_generate",
            "reqId": req_id,
            "fReq": freq,
            "model": "gemini-4-neon",
            "hl": "zh-CN",
            "hasCursor": False,
            "emitRawChunks": True,
            "extraHeaders": extra_h,
            "fallbackBl": PROTO_PROFILE_CACHE.resolve_fallback_bl(prof_snapshot=prof),
            "headersTimeoutMs": 90000,
            "firstWrbTimeoutMs": 120000,
            "idleTimeoutMs": 120000,
        }
        s.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
        buf = bytearray()
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf.extend(chunk)
            while b"\n" in buf:
                nl = buf.find(b"\n")
                ln = bytes(buf[:nl]).strip()
                del buf[: nl + 1]
                if not ln:
                    continue
                try:
                    ev = json.loads(ln.decode("utf-8", errors="replace"))
                except Exception:
                    continue
                if ev.get("event") == "raw_chunk":
                    raw_chunks.append(str(ev.get("data", "")))
                elif ev.get("event") in ("done", "error"):
                    break
            else:
                continue
            break
    finally:
        s.close()

    joined = "".join(raw_chunks)
    cand_slots_summary: Dict[str, str] = {}
    part_labels: List[str] = []
    for ln in joined.splitlines():
        ls = ln.strip()
        if not ls.startswith("[["):
            continue
        try:
            outer = json.loads(ls)
            for item in outer:
                if isinstance(item, list) and len(item) >= 3 and item[0] == "wrb.fr" and isinstance(item[2], str):
                    part = json.loads(item[2])
                    if isinstance(part, list):
                        if len(part) > 1 and isinstance(part[1], list) and part[1] and isinstance(part[1][0], str):
                            cid_seen = part[1][0]
                        for pi, pv in enumerate(part):
                            if isinstance(pv, str) and len(pv) < 80 and any(w in pv.lower() for w in ("neon", "carbon", "hydrogen", "flash", "gemini", "argon")):
                                part_labels.append(f"part[{pi}]={pv}")
                        if len(part) > 4 and isinstance(part[4], list) and part[4] and isinstance(part[4][0], list):
                            cand = part[4][0]
                            for ci, cv in enumerate(cand):
                                if cv is not None:
                                    s_cv = json.dumps(cv, ensure_ascii=False)
                                    cand_slots_summary[f"cand[{ci}]"] = s_cv[:240]
        except Exception:
            continue
    if cid_seen.startswith("c_"):
        unix_rpc(sock_path, {"op": "delete_conversations", "cids": [cid_seen]}, timeout=10.0)
    return {
        "raw_chars": len(joined),
        "cleaned_cid": cid_seen,
        "part_labels": sorted(set(part_labels)),
        "cand_slots": cand_slots_summary,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe full raw otAQ7b models, policy headers & Task/Chat flags.")
    ap.add_argument("--socket", default=DEFAULT_SOCK)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--smoke", action="store_true", help="Also live smoke-test all native models via gateway")
    ap.add_argument("--probe-neon-raw", action="store_true", help="Capture raw wrb.fr candidate slots for gemini-4-neon high")
    args = ap.parse_args()

    if not os.path.exists(args.socket):
        print(f"[probe] WebKit bridge socket not found: {args.socket} (run `./cc-demo.sh start` first)", file=sys.stderr)
        return 1

    st = {}
    for _ in range(15):
        st = unix_rpc(args.socket, {"op": "status"}, timeout=5.0)
        if st.get("ready"):
            break
        if st.get("page_loaded") and not st.get("logged_in"):
            time.sleep(1.5)
            st = unix_rpc(args.socket, {"op": "status"}, timeout=5.0)
            break
        time.sleep(0.8)
    st_safe = {k: v for k, v in st.items() if k != "account"}
    print("[probe] Bridge status:", json.dumps(st_safe, ensure_ascii=False))
    if not st.get("ready"):
        print("[probe] Bridge is not logged in / ready yet. Run `./cc-demo.sh login` first.", file=sys.stderr)
        return 2

    raw_probe = unix_rpc(args.socket, {"op": "probe_raw_discovery"}, timeout=25.0)
    ot_inner = raw_probe.get("otAQ7b_inner")
    hex_nodes = extract_hex_nodes(ot_inner) if ot_inner is not None else []

    policy_raw = None
    if isinstance(ot_inner, list):
        if len(ot_inner) > 24 and isinstance(ot_inner[24], list):
            policy_raw = ot_inner[24]
        elif ot_inner and isinstance(ot_inner[0], list) and len(ot_inner[0]) > 24:
            policy_raw = ot_inner[0][24]

    report: Dict[str, Any] = {
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "bl": raw_probe.get("bl", ""),
        "sniffed_headers": raw_probe.get("sniffed_headers", {}),
        "hex_nodes_count": len(hex_nodes),
        "hex_nodes": hex_nodes,
        "policy_raw_block": policy_raw,
        "wiz_hints": raw_probe.get("wiz_hints", {}),
        "ui_texts": raw_probe.get("ui_texts", []),
        "otAQ7b_inner": ot_inner,
    }

    print(f"[probe] Found {len(hex_nodes)} raw 16-hex model nodes in otAQ7b:")
    for hn in hex_nodes:
        print(f"  - {hn['hex']} @ {hn['path']} -> {json.dumps(hn['node'][:8], ensure_ascii=False)}")
    if isinstance(policy_raw, list) and len(policy_raw) >= 2:
        print(f"[probe] Policy block `{policy_raw[0]}` has {len(policy_raw[1])} entries:")
        for entry in policy_raw[1]:
            if isinstance(entry, list) and len(entry) >= 2:
                print(f"  - hdr={json.dumps(entry[0], ensure_ascii=False)} levels={json.dumps(entry[1], ensure_ascii=False)}")

    if args.probe_neon_raw:
        print("[probe] Capturing raw wrb.fr slots for gemini-4-neon (effort=high)...")
        neon_raw = probe_neon_raw_frames(args.socket)
        report["neon_raw_frames"] = neon_raw
        print("[probe] Neon raw part_labels:", json.dumps(neon_raw.get("part_labels", []), ensure_ascii=False))
        for k, v in sorted((neon_raw.get("cand_slots") or {}).items()):
            print(f"  {k}: {v}")

    if args.smoke:
        report["smoke_results"] = smoke_test_models(
            args.port,
            ["gemini-4-neon", "gemini-4-carbon", "gemini-4-hydrogen", "gemini-4-argon", "gemini-3.5-flash-lite"],
            ["low", "medium", "high"],
        )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
    print(f"[probe] Full sanitized report saved to: {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
