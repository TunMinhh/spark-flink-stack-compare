"""
Lightweight Pipeline A streaming preflight.

This script emits a tiny realtime burst and checks that data advances through:
Kafka -> Bronze Delta -> Silver Delta -> Gold Delta.
"""

from __future__ import annotations

import os
import time

import benchmark


SMOKE_USERS_PER_TICK = int(os.getenv("SMOKE_USERS_PER_TICK", "1"))
SMOKE_SECS = int(os.getenv("SMOKE_SECS", "10"))
SMOKE_WAIT_SECS = int(os.getenv("SMOKE_WAIT_SECS", "180"))
SMOKE_POLL_SECS = float(os.getenv("SMOKE_POLL_SECS", "5"))


def wait_for_rows(label: str, table_path: str, baseline_rows: int) -> bool:
    deadline = time.time() + SMOKE_WAIT_SECS
    while time.time() < deadline:
        rows = benchmark.delta_row_count(table_path)
        if rows > baseline_rows:
            print(f"  OK {label}: rows {baseline_rows} -> {rows}")
            return True
        print(f"  .. waiting {label}: rows still {rows}")
        time.sleep(SMOKE_POLL_SECS)
    print(f"  FAIL {label}: rows did not increase within {SMOKE_WAIT_SECS}s")
    return False


def wait_for_commit(label: str, table_path: str, baseline_ts: int) -> bool:
    deadline = time.time() + SMOKE_WAIT_SECS
    while time.time() < deadline:
        ts = benchmark.latest_delta_ts_ms(table_path)
        if ts > baseline_ts:
            print(f"  OK {label}: commit advanced")
            return True
        print(f"  .. waiting {label}: no new commit yet")
        time.sleep(SMOKE_POLL_SECS)
    print(f"  FAIL {label}: commit did not advance within {SMOKE_WAIT_SECS}s")
    return False


def main() -> None:
    print("[smoke] Pipeline A streaming preflight")
    print(f"  users_per_tick={SMOKE_USERS_PER_TICK} duration={SMOKE_SECS}s wait={SMOKE_WAIT_SECS}s")

    if not benchmark.verify_streaming_jobs():
        raise SystemExit("[smoke] Missing streaming jobs. Run: bash benchmark/run_benchmark.sh")

    base_b_rows = benchmark.delta_row_count(benchmark.BRONZE_WATCH)
    base_s_rows = benchmark.delta_row_count(benchmark.SILVER_WATCH)
    base_g_ts = benchmark.latest_delta_ts_ms(benchmark.GOLD_WATCH)
    print(f"  baseline rows: bronze={base_b_rows} silver={base_s_rows}")

    producer = benchmark.start_producer(SMOKE_USERS_PER_TICK)
    print(f"  [producer] running for {SMOKE_SECS}s ...")
    time.sleep(SMOKE_SECS)
    producer.terminate()
    producer.wait(timeout=30)

    checks = [
        wait_for_rows("Bronze", benchmark.BRONZE_WATCH, base_b_rows),
        wait_for_rows("Silver", benchmark.SILVER_WATCH, base_s_rows),
        wait_for_commit("Gold", benchmark.GOLD_WATCH, base_g_ts),
    ]
    if not all(checks):
        raise SystemExit("[smoke] FAILED")
    print("[smoke] OK: realtime path is moving end-to-end")


if __name__ == "__main__":
    main()
