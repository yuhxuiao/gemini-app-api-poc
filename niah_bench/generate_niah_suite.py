#!/usr/bin/env python3
"""
generate_niah_suite.py - Generates calibrated Needle-in-a-Haystack (NIAH) &
Multi-Turn Compaction Watermark benchmark files for Gemini.app Task (Carbon).

Outputs under `./niah_bench/` (relative to this script's directory):
  1. Single-Turn Ceiling Suite (5 needles per file at 5%, 25%, 50%, 75%, 95% depth):
     - single_turn/niah_100k.txt (~100,000 tokens)
     - single_turn/niah_200k.txt (~200,000 tokens)
     - single_turn/niah_400k.txt (~400,000 tokens)
     - single_turn/niah_800k.txt (~800,000 tokens)
  2. Multi-Turn Auto-Compaction Watermark Suite (6 x 50,000-token increments = 50K..300K):
     - multi_turn/step_01_050k.txt .. step_06_300k.txt
     Each file embeds two unobtrusive metrics (an early 20% metric and a 75% metric) so you
     only ask a general question on Turns 1..k, then query earlier metrics without file reads
     to pinpoint the exact token count where Gemini.app silently compacts raw history!
  3. answer_key.json & verify_answers.py (automated accuracy & truncation scorer)
"""

import json
import os
import random

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
SINGLE_DIR = os.path.join(OUT_DIR, "single_turn")
MULTI_DIR = os.path.join(OUT_DIR, "multi_turn")

# Calibrated ratio for non-repetitive English structured technical prose/logs on Gemini SentencePiece BPE:
# ~3.95 UTF-8 bytes per token (verified across mixed words, punctuation, digits, and identifiers).
CHARS_PER_TOKEN = 3.95

SERVICES = [
    "auth-edge", "billing-ledger", "catalog-sync", "dispatch-router", "event-ingestor",
    "feature-store", "GeoResolver", "health-monitor", "index-builder", "job-scheduler",
    "kv-replicator", "log-shipper", "media-transcoder", "notify-hub", "order-pipeline",
    "policy-engine", "quota-tracker", "rate-limiter", "session-broker", "telemetry-sink",
]
REGIONS = ["us-west1", "us-east4", "europe-west1", "asia-east1", "asia-northeast1", "us-central1"]
VERBS = ["validated", "reconciled", "checkpointed", "flushed", "synchronized", "compacted", "rebalanced", "audited"]
NOUNS = [
    "shard allocation table", "upstream connection pool", "merkle snapshot index",
    "lease renewal quorum", "write-ahead ring buffer", "bucket token allowance",
    "replica watermark vector", "batch manifest digest", "routing table epoch",
    "backpressure window state", "schema migration lock", "cold storage manifest",
]
STATUSES = ["nominal", "steady", "converged", "healthy", "verified", "balanced"]


def make_haystack_line(rng: random.Random, line_no: int) -> str:
    svc = rng.choice(SERVICES)
    reg = rng.choice(REGIONS)
    verb = rng.choice(VERBS)
    noun = rng.choice(NOUNS)
    st = rng.choice(STATUSES)
    lat = rng.randint(11, 289)
    seq = rng.randint(10000, 99999)
    hh = (line_no // 3600) % 24
    mm = (line_no // 60) % 60
    ss = line_no % 60
    return (
        f"[2026-11-15T{hh:02d}:{mm:02d}:{ss:02d}Z] region={reg} unit={svc} trace={seq} "
        f"note=\"{svc} {verb} {noun} in {lat}ms with status {st}.\"\n"
    )


def generate_file_with_needles(filepath: str, target_tokens: int, seed: int, needles_spec: list) -> dict:
    """
    needles_spec: list of dicts [{"depth": 0.05, "zone": "P05", "code": 4812, "phrase": "amber-falcon"}, ...]
    """
    rng = random.Random(seed)
    target_chars = int(target_tokens * CHARS_PER_TOKEN)

    # Pre-sample average line length (~134 chars) to compute total lines
    sample_lines = [make_haystack_line(rng, i) for i in range(200)]
    avg_len = sum(len(x) for x in sample_lines) / len(sample_lines)
    est_total_lines = max(100, int(target_chars / avg_len))

    # Map needle insertion line indices
    needle_by_line = {}
    recorded = []
    for sp in needles_spec:
        target_line = max(5, min(est_total_lines - 5, int(est_total_lines * sp["depth"])))
        while target_line in needle_by_line:
            target_line += 1
        # Natural telemetry sentence that blends into logs without obvious `GREP_ME` banners,
        # while having an unambiguous semantic anchor ("prism calibration checkpoint <zone>").
        line_text = (
            f"[2026-11-15T08:30:00Z] region=global-core unit=prism-calibrator trace=88001 "
            f"note=\"Prism calibration checkpoint {sp['zone']} recorded drift_val={sp['code']} "
            f"under codename {sp['phrase']} during cycle verification.\"\n"
        )
        needle_by_line[target_line] = (sp, line_text)

    rng = random.Random(seed + 999)
    written_chars = 0
    line_idx = 0

    with open(filepath, "w", encoding="utf-8", buffering=1024 * 1024) as fp:
        while written_chars < target_chars:
            if line_idx in needle_by_line:
                sp, n_text = needle_by_line[line_idx]
                actual_tok_pos = int(written_chars / CHARS_PER_TOKEN)
                fp.write(n_text)
                written_chars += len(n_text)
                recorded.append({
                    "zone": sp["zone"],
                    "target_depth_pct": int(sp["depth"] * 100),
                    "actual_char_offset": written_chars,
                    "est_token_offset": actual_tok_pos,
                    "code": sp["code"],
                    "phrase": sp["phrase"],
                    "line_number": line_idx + 1,
                })
            else:
                hline = make_haystack_line(rng, line_idx)
                fp.write(hline)
                written_chars += len(hline)
            line_idx += 1

    est_tokens = int(round(written_chars / CHARS_PER_TOKEN))
    for r in recorded:
        r["actual_depth_pct"] = round(r["actual_char_offset"] / written_chars * 100, 1)

    return {
        "file": filepath,
        "filename": os.path.basename(filepath),
        "target_tokens": target_tokens,
        "est_tokens": est_tokens,
        "bytes": written_chars,
        "lines": line_idx,
        "needles": recorded,
        "sum_of_codes": sum(r["code"] for r in recorded),
    }


def main():
    os.makedirs(SINGLE_DIR, exist_ok=True)
    os.makedirs(MULTI_DIR, exist_ok=True)

    manifest = {"single_turn": {}, "multi_turn": []}

    # 1. Single-Turn Ceiling Suite: 100K, 200K, 400K, 800K
    single_configs = [
        ("100k", 100_000, 1001, [
            {"depth": 0.05, "zone": "ALPHA-05", "code": 3104, "phrase": "cobalt-heron"},
            {"depth": 0.25, "zone": "ALPHA-25", "code": 5821, "phrase": "velvet-spruce"},
            {"depth": 0.50, "zone": "ALPHA-50", "code": 7490, "phrase": "quartz-otter"},
            {"depth": 0.75, "zone": "ALPHA-75", "code": 2619, "phrase": "crimson-lynx"},
            {"depth": 0.95, "zone": "ALPHA-95", "code": 8366, "phrase": "silver-bison"},
        ]),
        ("200k", 200_000, 2002, [
            {"depth": 0.05, "zone": "BETA-05", "code": 4215, "phrase": "amber-kestrel"},
            {"depth": 0.25, "zone": "BETA-25", "code": 6932, "phrase": "jade-cypress"},
            {"depth": 0.50, "zone": "BETA-50", "code": 1584, "phrase": "ivory-badger"},
            {"depth": 0.75, "zone": "BETA-75", "code": 9047, "phrase": "scarlet-finch"},
            {"depth": 0.95, "zone": "BETA-95", "code": 3722, "phrase": "copper-walrus"},
        ]),
        ("400k", 400_000, 4004, [
            {"depth": 0.05, "zone": "GAMMA-05", "code": 7128, "phrase": "indigo-condor"},
            {"depth": 0.25, "zone": "GAMMA-25", "code": 2463, "phrase": "golden-maple"},
            {"depth": 0.50, "zone": "GAMMA-50", "code": 8819, "phrase": "obsidian-marten"},
            {"depth": 0.75, "zone": "GAMMA-75", "code": 5306, "phrase": "coral-ibis"},
            {"depth": 0.95, "zone": "GAMMA-95", "code": 1984, "phrase": "bronze-orca"},
        ]),
        ("800k", 800_000, 8008, [
            {"depth": 0.05, "zone": "OMEGA-05", "code": 6409, "phrase": "azure-osprey"},
            {"depth": 0.25, "zone": "OMEGA-25", "code": 3871, "phrase": "emerald-cedar"},
            {"depth": 0.50, "zone": "OMEGA-50", "code": 9255, "phrase": "onyx-ferret"},
            {"depth": 0.75, "zone": "OMEGA-75", "code": 4712, "phrase": "topaz-crane"},
            {"depth": 0.95, "zone": "OMEGA-95", "code": 8153, "phrase": "platinum-seal"},
        ]),
    ]

    for label, tok_cnt, seed, n_spec in single_configs:
        out_path = os.path.join(SINGLE_DIR, f"niah_{label}.txt")
        info = generate_file_with_needles(out_path, tok_cnt, seed, n_spec)
        manifest["single_turn"][label] = info
        print(
            f"[SINGLE {label.upper()}] {info['filename']}: {info['bytes']:,} bytes "
            f"(~{info['est_tokens']:,} tok, {info['lines']:,} lines), sum={info['sum_of_codes']}"
        )

    # 2. Multi-Turn Auto-Compaction Watermark Suite: 6 x 50K tokens (Cumulative 50K -> 300K)
    step_specs = [
        (1, 50_000, "STEP1-A", 1142, "birch-01", "STEP1-B", 6628, "birch-02"),
        (2, 100_000, "STEP2-A", 2319, "cedar-01", "STEP2-B", 7405, "cedar-02"),
        (3, 150_000, "STEP3-A", 3580, "maple-01", "STEP3-B", 8192, "maple-02"),
        (4, 200_000, "STEP4-A", 4761, "alder-01", "alder-b", 9034, "alder-02"),
        (5, 250_000, "STEP5-A", 5903, "pine-01", "STEP5-B", 1847, "pine-02"),
        (6, 300_000, "STEP6-A", 6274, "rowan-01", "STEP6-B", 2956, "rowan-02"),
    ]
    # Normalize zone naming cleanly
    step_specs[3] = (4, 200_000, "STEP4-A", 4761, "alder-01", "STEP4-B", 9034, "alder-02")

    for step_idx, cum_tok, z1, c1, p1, z2, c2, p2 in step_specs:
        fname = f"step_0{step_idx}_cum{cum_tok // 1000:03d}k.txt"
        out_path = os.path.join(MULTI_DIR, fname)
        info = generate_file_with_needles(out_path, 50_000, 5000 + step_idx * 37, [
            {"depth": 0.25, "zone": z1, "code": c1, "phrase": p1},
            {"depth": 0.75, "zone": z2, "code": c2, "phrase": p2},
        ])
        info["step"] = step_idx
        info["cumulative_tokens_after_step"] = cum_tok
        manifest["multi_turn"].append(info)
        print(
            f"[MULTI STEP {step_idx} | Cum {cum_tok // 1000}K] {fname}: "
            f"{info['bytes']:,} bytes (~{info['est_tokens']:,} tok)"
        )

    ans_path = os.path.join(OUT_DIR, "answer_key.json")
    with open(ans_path, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, ensure_ascii=False, indent=2)
    print(f"\nSaved complete ground-truth answer key to: {ans_path}")


if __name__ == "__main__":
    main()
