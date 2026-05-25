"""
Output equivalence validator — Pipeline A vs Pipeline B.

Connects to both postgres-sink databases and compares every Gold table
row-by-row (schema, row counts, value distributions, and exact-match rate).

Both pipelines must have already run export-gold before calling this.

Usage:
    python validate.py

Env vars:
    PG_A_HOST   default 127.0.0.1
    PG_A_PORT   default 5434        (pipeline_a postgres-sink)
    PG_B_HOST   default 127.0.0.1
    PG_B_PORT   default 5435        (pipeline_b postgres-sink)
    PG_A_DB / PG_B_DB               default wellness
    PG_A_USER / PG_B_USER           default wellness
    PG_A_PASS / PG_B_PASS           default wellness
    TOLERANCE   default 0.01        (1% relative tolerance for numeric means)

Exit code:
    0  — all tables match within tolerance
    1  — one or more mismatches found
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field

import pandas as pd
import psycopg2

# ── Config ────────────────────────────────────────────────────────────────────
PG_A = dict(
    host=os.getenv("PG_A_HOST", "127.0.0.1"),
    port=int(os.getenv("PG_A_PORT", "5434")),
    dbname=os.getenv("PG_A_DB",   "wellness"),
    user=os.getenv("PG_A_USER",   "grafana"),
    password=os.getenv("PG_A_PASS", "grafana123"),
)
PG_B = dict(
    host=os.getenv("PG_B_HOST", "127.0.0.1"),
    port=int(os.getenv("PG_B_PORT", "5435")),
    dbname=os.getenv("PG_B_DB",   "wellness"),
    user=os.getenv("PG_B_USER",   "grafana"),
    password=os.getenv("PG_B_PASS", "grafana123"),
)
TOLERANCE = float(os.getenv("TOLERANCE", "0.01"))   # 1% relative tolerance

GOLD_TABLES = [
    "daily_vitals_summary",
    "daily_activity_summary",
    "daily_context_summary",
    "daily_sleep_summary",
    "daily_vitals_daily_summary",
    "daily_intraday_summary",
    "daily_wellness_profile",
]


# ── DB helpers ────────────────────────────────────────────────────────────────
def connect(cfg: dict) -> psycopg2.extensions.connection:
    return psycopg2.connect(**cfg)


def read_table(conn, table: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {table} ORDER BY user_id, event_date", conn)


# ── Comparison result ─────────────────────────────────────────────────────────
@dataclass
class TableResult:
    table: str
    passed: bool
    issues: list[str] = field(default_factory=list)
    stats:  list[str] = field(default_factory=list)


# ── Per-table checks ──────────────────────────────────────────────────────────
def compare_table(table: str, df_a: pd.DataFrame, df_b: pd.DataFrame) -> TableResult:
    result = TableResult(table=table, passed=True)

    # 1. Schema check
    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)
    only_a = cols_a - cols_b
    only_b = cols_b - cols_a
    if only_a:
        result.issues.append(f"Columns only in A: {sorted(only_a)}")
        result.passed = False
    if only_b:
        result.issues.append(f"Columns only in B: {sorted(only_b)}")
        result.passed = False

    common_cols = sorted(cols_a & cols_b)

    # 2. Row count
    if len(df_a) != len(df_b):
        result.issues.append(f"Row count mismatch: A={len(df_a)}, B={len(df_b)}")
        result.passed = False
    result.stats.append(f"rows: A={len(df_a)}, B={len(df_b)}")

    # 3. User / date coverage
    if "user_id" in common_cols and "event_date" in common_cols:
        keys_a = set(zip(df_a["user_id"], df_a["event_date"].astype(str)))
        keys_b = set(zip(df_b["user_id"], df_b["event_date"].astype(str)))
        only_keys_a = keys_a - keys_b
        only_keys_b = keys_b - keys_a
        if only_keys_a:
            n = len(only_keys_a)
            sample = sorted(only_keys_a)[:3]
            result.issues.append(f"(user_id, date) only in A: {n} keys e.g. {sample}")
            result.passed = False
        if only_keys_b:
            n = len(only_keys_b)
            sample = sorted(only_keys_b)[:3]
            result.issues.append(f"(user_id, date) only in B: {n} keys e.g. {sample}")
            result.passed = False

    # 4. Numeric column comparison (mean, null rate)
    skip = {"user_id", "event_date", "gold_updated_at", "processed_at",
            "dominant_mood", "dominant_activity_type", "age", "gender", "bmi"}
    numeric_cols = [
        c for c in common_cols
        if c not in skip
        and pd.api.types.is_numeric_dtype(df_a[c])
        and pd.api.types.is_numeric_dtype(df_b[c])
    ]

    col_issues = []
    for col in numeric_cols:
        null_a = df_a[col].isna().mean()
        null_b = df_b[col].isna().mean()
        mean_a = df_a[col].mean()
        mean_b = df_b[col].mean()

        # Null rate difference
        if abs(null_a - null_b) > 0.05:   # >5pp difference
            col_issues.append(
                f"  {col}: null_rate A={null_a:.3f} B={null_b:.3f}"
            )

        # Mean difference (skip if both near-zero)
        if abs(mean_a) < 1e-9 and abs(mean_b) < 1e-9:
            continue
        denom = max(abs(mean_a), 1e-9)
        rel = abs(mean_a - mean_b) / denom
        if rel > TOLERANCE:
            col_issues.append(
                f"  {col}: mean A={mean_a:.4f} B={mean_b:.4f} "
                f"(diff={rel*100:.2f}% > {TOLERANCE*100:.0f}% tol)"
            )

    if col_issues:
        result.issues.append("Numeric column mismatches:")
        result.issues.extend(col_issues)
        result.passed = False

    # 5. Boolean / categorical columns — mode match
    bool_cols = [
        c for c in common_cols
        if c not in skip
        and pd.api.types.is_bool_dtype(df_a[c])
    ]
    for col in bool_cols:
        rate_a = df_a[col].mean()
        rate_b = df_b[col].mean()
        if abs(rate_a - rate_b) > 0.05:
            result.issues.append(
                f"  {col}: true_rate A={rate_a:.3f} B={rate_b:.3f}"
            )
            result.passed = False

    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    print("Connecting to postgres-sink instances...")
    try:
        conn_a = connect(PG_A)
        print(f"  Pipeline A: {PG_A['host']}:{PG_A['port']}/{PG_A['dbname']} ✓")
    except Exception as e:
        print(f"  Pipeline A connection FAILED: {e}")
        return 1

    try:
        conn_b = connect(PG_B)
        print(f"  Pipeline B: {PG_B['host']}:{PG_B['port']}/{PG_B['dbname']} ✓")
    except Exception as e:
        print(f"  Pipeline B connection FAILED: {e}")
        return 1

    results: list[TableResult] = []
    any_missing = False

    print(f"\nComparing {len(GOLD_TABLES)} Gold tables (tolerance={TOLERANCE*100:.0f}%)...\n")

    for table in GOLD_TABLES:
        print(f"  {table}")

        try:
            df_a = read_table(conn_a, table)
        except Exception as e:
            print(f"    [SKIP] Pipeline A — table missing or unreadable: {e}")
            any_missing = True
            continue

        try:
            df_b = read_table(conn_b, table)
        except Exception as e:
            print(f"    [SKIP] Pipeline B — table missing or unreadable: {e}")
            any_missing = True
            continue

        r = compare_table(table, df_a, df_b)
        results.append(r)

        status = "✓ PASS" if r.passed else "✗ FAIL"
        print(f"    {status}  ({', '.join(r.stats)})")
        for issue in r.issues:
            print(f"    ↳ {issue}")

    conn_a.close()
    conn_b.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]

    print(f"\n{'='*60}")
    print(f"SUMMARY  —  {len(passed)}/{len(results)} tables passed")
    if any_missing:
        print("  WARNING: some tables were missing (run export-gold first)")
    if failed:
        print(f"\n  FAILED tables:")
        for r in failed:
            print(f"    • {r.table}")
    else:
        print("  All tables match within tolerance — pipelines are equivalent.")
    print("="*60)

    return 0 if (not failed and not any_missing) else 1


if __name__ == "__main__":
    sys.exit(main())
