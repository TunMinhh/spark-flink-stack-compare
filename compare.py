"""
Cross-pipeline benchmark comparison.

Reads the latest results CSV from pipeline_a/benchmark/ and pipeline_b/benchmark/
and prints a side-by-side table suitable for copying into a paper.

Usage:
    python compare.py                         # auto-picks newest CSV from each pipeline
    python compare.py --a path/a.csv --b path/b.csv   # explicit files

Output columns (mean ± SD across runs, per load tier):
    req/s | Bronze A | Bronze B | Silver A | Silver B | Gold A | Gold B | E2E A | E2E B | RAM A | RAM B
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
from statistics import mean, stdev
from pathlib import Path


# ── CSV loading ───────────────────────────────────────────────────────────────
def latest_csv(pipeline_dir: str) -> str:
    pattern = os.path.join(pipeline_dir, "benchmark", "results_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No results CSV found in {pipeline_dir}/benchmark/")
    return files[-1]


def load_csv(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _float(val: str) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return -1.0


def _bool(val: str) -> bool:
    return str(val).strip().lower() in ("true", "1", "yes")


# ── Aggregation ───────────────────────────────────────────────────────────────
def aggregate(rows: list[dict]) -> dict[float, dict]:
    """Group by req_per_sec, return stats per rate."""
    by_rate: dict[float, list[dict]] = {}
    for r in rows:
        rate = _float(r["req_per_sec"])
        by_rate.setdefault(rate, []).append(r)

    result = {}
    for rate, runs in sorted(by_rate.items()):
        def _stat(col: str, ok_col: str) -> tuple[float, float]:
            vals = [_float(r[col]) for r in runs
                    if _bool(r.get(ok_col, "true")) and _float(r[col]) > 0]
            if not vals:
                return -1.0, -1.0
            return mean(vals), (stdev(vals) if len(vals) > 1 else 0.0)

        result[rate] = {
            "bronze": _stat("bronze_duration_s", "bronze_ok"),
            "silver": _stat("silver_duration_s", "silver_ok"),
            "gold":   _stat("gold_duration_s",   "gold_ok"),
            "e2e":    _stat("gold_e2e_s",         "gold_ok"),
            "ram":    _stat("engine_ram_mb",       "gold_ok"),
            "n":      len(runs),
        }
    return result


# ── Formatting ────────────────────────────────────────────────────────────────
def _ms(mean_s: float, sd_s: float) -> str:
    """Format mean ± SD in seconds; show N/A if no data."""
    if mean_s < 0:
        return "N/A"
    if sd_s > 0:
        return f"{mean_s:.1f}±{sd_s:.1f}s"
    return f"{mean_s:.1f}s"


def _mram(mean_mb: float, _sd: float) -> str:
    if mean_mb < 0:
        return "N/A"
    if mean_mb >= 1024:
        return f"{mean_mb/1024:.1f} GiB"
    return f"{mean_mb:.0f} MiB"


def _pct_diff(a: float, b: float) -> str:
    """Show B vs A as % change. Negative = B is faster/smaller."""
    if a <= 0 or b <= 0:
        return ""
    diff = (b - a) / a * 100
    sign = "+" if diff > 0 else ""
    return f"({sign}{diff:.0f}%)"


# ── Main table ────────────────────────────────────────────────────────────────
def print_table(agg_a: dict, agg_b: dict) -> None:
    all_rates = sorted(set(agg_a) | set(agg_b))

    col = 13
    hdr = (
        f"{'req/s':>7} | "
        f"{'Bronze A':>{col}} {'Bronze B':>{col}} | "
        f"{'Silver A':>{col}} {'Silver B':>{col}} | "
        f"{'Gold A':>{col}} {'Gold B':>{col}} | "
        f"{'E2E A':>{col}} {'E2E B':>{col}} | "
        f"{'RAM A':>9} {'RAM B':>9}"
    )
    print("\n" + "=" * len(hdr))
    print(hdr)
    print("-" * len(hdr))

    for rate in all_rates:
        a = agg_a.get(rate, {})
        b = agg_b.get(rate, {})

        def _cell_pair(key: str, fmt_fn) -> str:
            av, asd = a.get(key, (-1, -1))
            bv, bsd = b.get(key, (-1, -1))
            ca = fmt_fn(av, asd)
            cb = fmt_fn(bv, bsd)
            diff = _pct_diff(av, bv)
            return f"{ca:>{col}} {cb + ' ' + diff:>{col}}"

        row = (
            f"{rate:>7.0f} | "
            f"{_cell_pair('bronze', _ms)} | "
            f"{_cell_pair('silver', _ms)} | "
            f"{_cell_pair('gold',   _ms)} | "
            f"{_cell_pair('e2e',    _ms)} | "
            f"{_mram(*a.get('ram', (-1,-1))):>9} "
            f"{_mram(*b.get('ram', (-1,-1))):>9}"
        )
        print(row)

    print("=" * len(hdr))
    print("\nNotes:")
    print("  - Bronze A = Spark Structured Streaming time-to-first-commit (30s micro-batch)")
    print("  - Bronze B = Flink batch job duration (reads accumulated Kafka messages)")
    print("  - E2E      = producer start → Gold tables written")
    print("  - RAM      = engine container peak (spark-worker / flink-taskmanager)")
    print("  - % diff   = (B - A) / A × 100  [negative = Pipeline B faster/smaller]")


def print_latex(agg_a: dict, agg_b: dict) -> None:
    """Minimal LaTeX table for copy-paste into paper."""
    all_rates = sorted(set(agg_a) | set(agg_b))
    print("\n% ---- LaTeX table (paste into paper) ----")
    print(r"\begin{tabular}{r|cc|cc|cc|cc}")
    print(r"\hline")
    print(r"req/s & \multicolumn{2}{c|}{Silver (s)} & \multicolumn{2}{c|}{Gold (s)}"
          r" & \multicolumn{2}{c|}{E2E (s)} & \multicolumn{2}{c}{RAM (MiB)} \\")
    print(r"& A & B & A & B & A & B & A & B \\")
    print(r"\hline")
    for rate in all_rates:
        a = agg_a.get(rate, {})
        b = agg_b.get(rate, {})
        def v(d, key):
            m, _ = d.get(key, (-1, -1))
            return f"{m:.1f}" if m > 0 else "--"
        print(f"{rate:.0f} & {v(a,'silver')} & {v(b,'silver')} & "
              f"{v(a,'gold')} & {v(b,'gold')} & "
              f"{v(a,'e2e')} & {v(b,'e2e')} & "
              f"{v(a,'ram')} & {v(b,'ram')} \\\\")
    print(r"\hline")
    print(r"\end{tabular}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Compare Pipeline A vs B benchmark results")
    parser.add_argument("--a",     help="Path to Pipeline A results CSV")
    parser.add_argument("--b",     help="Path to Pipeline B results CSV")
    parser.add_argument("--latex", action="store_true", help="Also print LaTeX table")
    args = parser.parse_args()

    root = Path(__file__).parent
    path_a = args.a or latest_csv(str(root / "pipeline_a"))
    path_b = args.b or latest_csv(str(root / "pipeline_b"))

    print(f"Pipeline A: {path_a}")
    print(f"Pipeline B: {path_b}")

    rows_a = load_csv(path_a)
    rows_b = load_csv(path_b)

    # Filter to only this pipeline's rows (in case CSVs were merged)
    rows_a = [r for r in rows_a if r.get("pipeline", "A") == "A"]
    rows_b = [r for r in rows_b if r.get("pipeline", "B") == "B"]

    # Exclude warmup runs from the comparison (the CSV keeps them for transparency).
    def _is_real(r: dict) -> bool:
        return str(r.get("is_warmup", "")).strip().lower() not in ("true", "1", "yes")
    rows_a = [r for r in rows_a if _is_real(r)]
    rows_b = [r for r in rows_b if _is_real(r)]

    agg_a = aggregate(rows_a)
    agg_b = aggregate(rows_b)

    print_table(agg_a, agg_b)

    if args.latex:
        print_latex(agg_a, agg_b)


if __name__ == "__main__":
    main()
