"""
Recompute avg/max staleness trimmed to post-first-Gold samples.

Original harness measured staleness from t=0 of each run, so Phase 1
(pre-first-Gold-commit) was inflated by the inter-run gap.  This script
applies the same filter the fixed harness uses: only samples with
wall_s >= first_gold_latency_s count toward avg/max/min.

Usage:
    python recompute_staleness.py
Outputs corrected numbers to stdout and writes
    benchmark_result/staleness_corrected.csv
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from statistics import mean
from pathlib import Path

BASE = Path(__file__).parent
RATES = [50, 100, 200]
PIPELINES = [
    ("spark", "A", "spark_result"),
    ("flink", "B", "flink_result"),
]


def load_results(path: Path) -> dict[str, float]:
    """Returns {run_label: first_gold_latency_s} for all rows."""
    out: dict[str, float] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            pipeline  = row["pipeline"]
            run       = row["run"]
            rps       = row["req_per_sec"]
            label     = f"{pipeline}_rate{rps}_run{run}"
            try:
                out[label] = float(row["first_gold_latency_s"])
            except (ValueError, KeyError):
                out[label] = 0.0
    return out


def load_staleness(path: Path) -> dict[str, list[dict]]:
    """Returns {run_label: [sample_dict, ...]}."""
    out: dict[str, list[dict]] = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            out[row["run_label"]].append({
                "wall_s":      float(row["wall_s"]),
                "staleness_s": float(row["staleness_s"]),
                "is_warmup":   row["is_warmup"],
            })
    return out


def stats(vals: list[float]) -> tuple[float, float, float]:
    if not vals:
        return -1.0, -1.0, -1.0
    return round(mean(vals), 2), round(max(vals), 2), round(min(vals), 2)


rows_out: list[dict] = []

print(f"{'Rate':>6}  {'Pipeline':>8}  {'Run':>4}  {'Warmup':>6}  "
      f"{'Orig avg':>9}  {'Orig max':>9}  "
      f"{'Corr avg':>9}  {'Corr max':>9}  "
      f"{'first_gold':>10}  {'n_total':>7}  {'n_post':>6}")
print("-" * 100)

for rate in RATES:
    for name, pipeline_id, folder in PIPELINES:
        res_path   = BASE / folder / f"{rate}_result.csv"
        stal_path  = BASE / folder / f"{rate}_staleness.csv"
        if not res_path.exists() or not stal_path.exists():
            print(f"  [SKIP] {name} rate={rate}: files not found")
            continue

        first_gold_map = load_results(res_path)
        staleness_map  = load_staleness(stal_path)

        for run_label, samples in sorted(staleness_map.items()):
            first_gold = first_gold_map.get(run_label, 0.0)
            is_warmup  = samples[0]["is_warmup"] if samples else "?"

            all_vals  = [s["staleness_s"] for s in samples]
            post_vals = [s["staleness_s"] for s in samples if s["wall_s"] >= first_gold]

            orig_avg, orig_max, orig_min = stats(all_vals)
            corr_avg, corr_max, corr_min = stats(post_vals)

            print(f"{rate:>6}  {name:>8}  {run_label.split('_run')[-1]:>4}  "
                  f"{is_warmup:>6}  "
                  f"{orig_avg:>9}  {orig_max:>9}  "
                  f"{corr_avg:>9}  {corr_max:>9}  "
                  f"{first_gold:>10}  {len(all_vals):>7}  {len(post_vals):>6}")

            rows_out.append({
                "rate":       rate,
                "pipeline":   name,
                "run_label":  run_label,
                "is_warmup":  is_warmup,
                "first_gold_latency_s": first_gold,
                "n_samples_total":   len(all_vals),
                "n_samples_post_first_gold": len(post_vals),
                "orig_avg_s": orig_avg,
                "orig_max_s": orig_max,
                "orig_min_s": orig_min,
                "corr_avg_s": corr_avg,
                "corr_max_s": corr_max,
                "corr_min_s": corr_min,
            })

# ── Summary: non-warmup runs only ────────────────────────────────────────────
print("\n" + "=" * 100)
print("SUMMARY (non-warmup runs, mean across runs per pipeline+rate)\n")
print(f"{'Rate':>6}  {'Pipeline':>8}  "
      f"{'Orig avg':>9}  {'Orig max':>9}  "
      f"{'Corr avg':>9}  {'Corr max':>9}  "
      f"{'Reduction':>10}")
print("-" * 75)

for rate in RATES:
    for name, _, _ in PIPELINES:
        non_warmup = [r for r in rows_out
                      if r["rate"] == rate
                      and r["pipeline"] == name
                      and r["is_warmup"] == "False"]
        if not non_warmup:
            continue
        oa = round(mean(r["orig_avg_s"] for r in non_warmup), 1)
        om = round(mean(r["orig_max_s"] for r in non_warmup), 1)
        ca = round(mean(r["corr_avg_s"] for r in non_warmup), 1)
        cm = round(mean(r["corr_max_s"] for r in non_warmup), 1)
        reduction = f"{oa/ca:.1f}x" if ca > 0 else "n/a"
        print(f"{rate:>6}  {name:>8}  {oa:>9}  {om:>9}  {ca:>9}  {cm:>9}  {reduction:>10}")

# ── Write CSV ─────────────────────────────────────────────────────────────────
out_path = BASE / "staleness_corrected.csv"
if rows_out:
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"\nWrote {len(rows_out)} rows → {out_path}")
