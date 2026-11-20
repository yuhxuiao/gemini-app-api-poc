#!/usr/bin/env python3
"""
verify_answers.py - Automatically scores Gemini.app Task (Carbon) responses against
`answer_key.json` to diagnose exact context window retention, head/middle/tail
truncation patterns, and exact multi-turn auto-compaction trigger watermarks.

Usage:
  python3 verify_answers.py --mode 400k --reply "ALPHA/GAMMA output pasted from Gemini.app..."
  python3 verify_answers.py --summary   # Prints the ground-truth reference table
"""

import argparse
import json
import os
import re

ANS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "answer_key.json")


def print_summary(data: dict) -> None:
    print("=== 1. Single-Turn NIAH Ground-Truth Checkpoints ===")
    for scale, info in data["single_turn"].items():
        print(f"\n[{scale.upper()}] {info['filename']} | {info['bytes']:,} B | ~{info['est_tokens']:,} tokens | Sum={info['sum_of_codes']}")
        for n in info["needles"]:
            print(
                f"  - Depth {n['actual_depth_pct']:4.1f}% (~{n['est_token_offset']:,} tok, L{n['line_number']}): "
                f"zone={n['zone']} | drift_val={n['code']} | codename={n['phrase']}"
            )

    print("\n=== 2. Multi-Turn Auto-Compaction Watermark Checkpoints ===")
    for st in data["multi_turn"]:
        n0, n1 = st["needles"][0], st["needles"][1]
        print(
            f"  Step {st['step']} (Cum ~{st['cumulative_tokens_after_step']//1000}K tok) | {st['filename']}: "
            f"{n0['zone']}={n0['code']} ({n0['phrase']}), {n1['zone']}={n1['code']} ({n1['phrase']})"
        )


def score_single(info: dict, reply_text: str) -> None:
    print(f"Scoring response for {info['filename']} (~{info['est_tokens']:,} tokens):")
    hits = []
    for n in info["needles"]:
        has_code = str(n["code"]) in reply_text
        has_phrase = n["phrase"].lower() in reply_text.lower()
        ok = has_code and has_phrase
        hits.append(ok)
        status = "HIT " if ok else ("PARTIAL" if (has_code or has_phrase) else "MISS")
        print(f"  [{status}] Depth {n['actual_depth_pct']:4.1f}% ({n['zone']}): expected {n['code']} / {n['phrase']}")

    has_sum = str(info["sum_of_codes"]) in reply_text.replace(",", "")
    print(f"  [{'HIT ' if has_sum else 'MISS'}] Checksum sum_of_codes = {info['sum_of_codes']}")

    if all(hits):
        print("=> VERDICT: 100% FULL-WINDOW RETENTION (No truncation or middle loss!)")
    elif hits[0] and hits[-1] and not all(hits[1:-1]):
        print("=> VERDICT: HEAD+TAIL SLICING DETECTED (Middle tokens were truncated!)")
    elif any(hits) and not hits[-1]:
        last_ok = max(i for i, v in enumerate(hits) if v)
        cutoff_tok = info["needles"][last_ok]["est_token_offset"]
        print(f"=> VERDICT: TAIL TRUNCATION DETECTED after ~{cutoff_tok:,} tokens!")
    elif any(hits) and not hits[0]:
        print("=> VERDICT: SLIDING-WINDOW HEAD DROP DETECTED (Earliest tokens dropped!)")
    else:
        print("=> VERDICT: 0% RECALL (File rejected or not loaded into model context)")


def main():
    with open(ANS_PATH, "r", encoding="utf-8") as fp:
        data = json.load(fp)
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", action="store_true", help="Print full ground-truth reference key")
    ap.add_argument("--mode", choices=["100k", "200k", "400k", "800k"], help="Score a single-turn reply")
    ap.add_argument("--reply", default="", help="Model reply text to score")
    args = ap.parse_args()

    if args.summary or not args.mode:
        print_summary(data)
        return
    score_single(data["single_turn"][args.mode], args.reply)


if __name__ == "__main__":
    main()
