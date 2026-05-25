"""
Pipeline B — Flink streaming benchmark (v2)

Three key dimensions measured:
  1. Gold STALENESS — how fresh is the Gold layer at any moment?
     A background thread polls Gold's Iceberg snapshot timestamp every
     STALENESS_POLL_SECS and records (wall_time, staleness_seconds).
     Flink streaming keeps staleness low (≤ checkpoint_interval ≈ 30s).

  2. End-to-end latency — producer START → Gold snapshot stable.
     After producer stops, we wait for each layer's snapshot to stabilise.

  3. Throughput — rows added per rate tier; find the saturation point
     by observing whether Gold catch-up lag grows with load.

Output files:
  benchmark/results_TIMESTAMP.csv   — one row per run (summary metrics)
  benchmark/staleness_TIMESTAMP.csv — timeseries: (wall_s, staleness_s) per run

Usage (run from pipeline_b/ directory):
    python benchmark/benchmark.py

Env vars:
    ICEBERG_REST_URL      http://localhost:8181
    FLINK_CONTAINER       pipeline_b-flink-jobmanager-1
    KAFKA_BOOTSTRAP       127.0.0.1:9092
    N_RUNS                3
    WARMUP_RUNS           1
    WARMUP_SECS           10   (producer burst duration per run)
    REQUEST_RATES         "50,100,200"
    DELAY                 0.1
    STALENESS_POLL_SECS   5.0
    STABLE_POLL_SECS      3.0
    STABLE_POLLS_REQUIRED 3
    STABLE_MAX_WAIT       600
"""

from __future__ import annotations

import csv
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from statistics import mean, stdev

import requests

# ── Config ────────────────────────────────────────────────────────────────────
ICEBERG_REST_URL      = os.getenv("ICEBERG_REST_URL",   "http://localhost:8181")
FLINK_CONTAINER       = os.getenv("FLINK_CONTAINER",    "pipeline_b-flink-jobmanager-1")
KAFKA_BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP",    "127.0.0.1:9092")
N_RUNS                = int(os.getenv("N_RUNS",         "3"))
WARMUP_RUNS           = int(os.getenv("WARMUP_RUNS",    "1"))
WARMUP_SECS           = int(os.getenv("WARMUP_SECS",    "10"))
DELAY                 = float(os.getenv("DELAY",        "0.1"))
PARALLELISM           = int(os.getenv("PARALLELISM",    "6"))
KAFKA_PARTITIONS      = os.getenv("KAFKA_PARTITIONS",   "12")
MEM_SAMPLE_SECS       = float(os.getenv("MEM_SAMPLE_SECS",     "2.0"))
STALENESS_POLL_SECS   = float(os.getenv("STALENESS_POLL_SECS", "5.0"))
STABLE_POLL_SECS      = float(os.getenv("STABLE_POLL_SECS",    "3.0"))
STABLE_POLLS_REQUIRED = int(os.getenv("STABLE_POLLS_REQUIRED", "3"))
STABLE_MAX_WAIT       = int(os.getenv("STABLE_MAX_WAIT",        "800"))
# Flink Gold checkpoint interval (seconds). Used to size the post-Silver settle
# window so Gold has time to commit its final snapshot before we record t_gold.
# Must match FLINK_CHECKPOINT_INTERVAL in flink_gold.py (default "15 s").
GOLD_CHECKPOINT_SECONDS = int(os.getenv("GOLD_CHECKPOINT_SECONDS", "15"))

_rates_env    = os.getenv("REQUEST_RATES", "50,100,200")
REQUEST_RATES = [int(x.strip()) for x in _rates_env.split(",")]
MAX_TICKS = int(os.getenv("MAX_TICKS", str(max(1, round(WARMUP_SECS / DELAY)))))
PRODUCER_TIMEOUT_SECS = int(os.getenv("PRODUCER_TIMEOUT_SECS", str(max(60, int(WARMUP_SECS * 6)))))

# Watch tables — must match producer_realtime.py output (intraday topics only)
INTRADAY_TABLES = ("heart_rate_intraday", "hrv_intraday", "breathing_intraday")
BRONZE_WATCH_TABLES = [("bronze", table) for table in INTRADAY_TABLES]
SILVER_WATCH_TABLES = [("silver", table) for table in INTRADAY_TABLES]
GOLD_WATCH   = ("gold",   "daily_intraday_summary")

_ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_FILE   = f"benchmark/results_{_ts}.csv"
STALENESS_FILE = f"benchmark/staleness_{_ts}.csv"


# ── Iceberg REST helpers ───────────────────────────────────────────────────────
def _table_metadata(ns: str, table: str) -> dict | None:
    try:
        r = requests.get(
            f"{ICEBERG_REST_URL}/v1/namespaces/{ns}/tables/{table}", timeout=10
        )
        return r.json().get("metadata", {}) if r.status_code == 200 else None
    except Exception:
        return None


def latest_snapshot_ts(ns: str, table: str) -> int:
    """Return current-snapshot timestamp-ms, or 0 if table/snapshot missing."""
    md = _table_metadata(ns, table)
    if not md:
        return 0
    cid = md.get("current-snapshot-id")
    if cid is None or cid == -1:
        return 0
    for snap in md.get("snapshots", []):
        if snap.get("snapshot-id") == cid:
            return int(snap.get("timestamp-ms", 0))
    return 0


def snapshot_row_count(ns: str, table: str) -> int:
    """Return live row count from current snapshot summary, or 0.

    For Iceberg V2 tables with write.upsert.enabled=true, Flink writes updates
    as new data files + equality delete files. The snapshot summary field
    'total-records' counts ALL data-file rows without subtracting equality
    deletes, so it grows on every UPSERT run even though the live row count
    stays constant. Subtracting 'total-equality-deletes' gives the correct
    number of live (non-deleted) rows.
    """
    md = _table_metadata(ns, table)
    if not md:
        return 0
    cid = md.get("current-snapshot-id")
    if cid is None or cid == -1:
        return 0
    for snap in md.get("snapshots", []):
        if snap.get("snapshot-id") == cid:
            try:
                summary = snap.get("summary", {})
                total = int(summary.get("total-records", 0))
                eq_deletes = int(summary.get("total-equality-deletes", 0))
                return max(0, total - eq_deletes)
            except (ValueError, TypeError):
                return 0
    return 0


def snapshot_row_count_many(tables: list[tuple[str, str]]) -> int:
    return sum(snapshot_row_count(ns, table) for ns, table in tables)


def latest_snapshot_ts_many(tables: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    return {(ns, table): latest_snapshot_ts(ns, table) for ns, table in tables}


def snapshot_row_count_by_table(tables: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    return {(ns, table): snapshot_row_count(ns, table) for ns, table in tables}


def wait_for_stable(
    ns: str,
    table: str,
    baseline_ts: int,
    label: str = "",
    baseline_rows: int | None = None,
) -> tuple[float, int, bool]:
    """
    Block until snapshot timestamp advances past baseline and stays unchanged
    for STABLE_POLLS_REQUIRED consecutive polls. When baseline_rows is provided,
    also require row count to advance and stabilise.
    Returns (seconds_waited, final_ts, ok).
    """
    start     = time.time()
    last_ts   = 0
    last_rows: int | None = None
    stable_n  = 0
    while time.time() - start < STABLE_MAX_WAIT:
        ts = latest_snapshot_ts(ns, table)
        rows = snapshot_row_count(ns, table) if baseline_rows is not None else None
        has_new_commit = ts > baseline_ts
        has_new_rows = rows is None or rows > baseline_rows
        stable_value = rows if rows is not None else ts
        if has_new_commit and has_new_rows:
            if stable_value == (last_rows if rows is not None else last_ts):
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    waited = round(time.time() - start, 2)
                    print(f"    [stable] {label or f'{ns}.{table}'} → {waited}s")
                    return waited, ts, True
            else:
                last_ts  = ts
                last_rows = rows
                stable_n = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label or f'{ns}.{table}'} did not stabilise within {STABLE_MAX_WAIT}s")
    return STABLE_MAX_WAIT, last_ts, False


def wait_for_stable_many(
    tables: list[tuple[str, str]],
    baseline_ts: dict[tuple[str, str], int],
    baseline_rows: dict[tuple[str, str], int],
    label: str,
    expected_delta_rows: int | None = None,
) -> tuple[float, int, bool]:
    """
    Block until every watched table has a newer snapshot, every watched table
    has more rows than its baseline, and the aggregate row count stops moving.
    """
    start = time.time()
    baseline_total_rows = sum(baseline_rows.values())
    expected_delta_per_table = (
        expected_delta_rows // len(tables)
        if expected_delta_rows is not None and tables
        else None
    )
    last_total_rows: int | None = None
    last_min_ts = 0
    stable_n = 0
    while time.time() - start < STABLE_MAX_WAIT:
        current_ts = latest_snapshot_ts_many(tables)
        current_rows = snapshot_row_count_by_table(tables)
        all_new_snapshots = all(current_ts[key] > baseline_ts[key] for key in tables)
        all_new_rows = all(current_rows[key] > baseline_rows[key] for key in tables)
        total_rows = sum(current_rows.values())
        enough_rows = (
            expected_delta_rows is None
            or (
                total_rows - baseline_total_rows >= expected_delta_rows
                and all(
                    current_rows[key] - baseline_rows[key] >= expected_delta_per_table
                    for key in tables
                )
            )
        )
        if all_new_snapshots and all_new_rows and enough_rows:
            if total_rows == last_total_rows:
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    waited = round(time.time() - start, 2)
                    final_ts = min(current_ts.values()) if current_ts else 0
                    print(f"    [stable] {label} → {waited}s")
                    return waited, final_ts, True
            else:
                last_total_rows = total_rows
                last_min_ts = min(current_ts.values()) if current_ts else 0
                stable_n = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label} did not stabilise within {STABLE_MAX_WAIT}s")
    return STABLE_MAX_WAIT, last_min_ts, False


# ── Staleness Monitor ─────────────────────────────────────────────────────────
class StalenessMonitor:
    """
    Polls Gold's latest Iceberg snapshot timestamp every STALENESS_POLL_SECS.
    Staleness = now() - snapshot_timestamp.

    Pipeline B (Flink streaming): staleness stays low, ≤ checkpoint_interval ≈ 30s,
    because Flink continuously commits new snapshots as data arrives.

    Use as a context manager; attach .samples to the shared staleness list after exit.
    """

    def __init__(self, ns: str, table: str, run_label: str = ""):
        self.ns        = ns
        self.table     = table
        self.run_label = run_label
        self.samples: list[dict] = []
        self._stop   = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0: float = 0.0

    def __enter__(self) -> "StalenessMonitor":
        self._t0     = time.time()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            ts_ms = latest_snapshot_ts(self.ns, self.table)
            if ts_ms > 0:
                staleness = max(0.0, time.time() - ts_ms / 1000.0)
                self.samples.append({
                    "wall_s":      round(time.time() - self._t0, 1),
                    "staleness_s": round(staleness, 1),
                })
            self._stop.wait(STALENESS_POLL_SECS)

    # Convenience stats
    def _vals(self) -> list[float]:
        return [s["staleness_s"] for s in self.samples]

    @property
    def avg_s(self) -> float:
        v = self._vals(); return round(mean(v), 2) if v else -1.0

    @property
    def max_s(self) -> float:
        v = self._vals(); return round(max(v), 2) if v else -1.0

    @property
    def min_s(self) -> float:
        v = self._vals(); return round(min(v), 2) if v else -1.0


# ── Memory sampler ────────────────────────────────────────────────────────────
def _docker_mem_mb() -> dict[str, float]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
        capture_output=True, text=True,
    )
    out: dict[str, float] = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        name     = parts[0].replace("pipeline_b-", "").replace("-1", "")
        used_str = parts[1].split("/")[0].strip()
        try:
            if "GiB" in used_str:
                mb = float(used_str.replace("GiB", "")) * 1024
            elif "MiB" in used_str:
                mb = float(used_str.replace("MiB", ""))
            elif "kB" in used_str:
                mb = float(used_str.replace("kB", "")) / 1024
            else:
                mb = -1.0
        except ValueError:
            mb = -1.0
        out[name] = round(mb, 1)
    return out


class MemorySampler:
    def __init__(self, interval: float = MEM_SAMPLE_SECS):
        self.interval = interval
        self._peaks: dict[str, float] = {}
        self._stop   = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "MemorySampler":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                for name, mb in _docker_mem_mb().items():
                    if mb > self._peaks.get(name, -1):
                        self._peaks[name] = mb
            except Exception:
                pass
            self._stop.wait(self.interval)

    def peak(self, substring: str) -> float:
        return max(
            (mb for name, mb in self._peaks.items() if substring in name),
            default=-1.0,
        )


# ── Flink job check ───────────────────────────────────────────────────────────
def verify_streaming_jobs() -> bool:
    print("[preflight] Verifying Bronze / Silver / Gold jobs are RUNNING …")
    try:
        result = subprocess.run(
            ["docker", "exec", FLINK_CONTAINER, "/opt/flink/bin/flink", "list"],
            capture_output=True, text=True, timeout=30,
        )
        names = []
        for line in result.stdout.splitlines():
            if "RUNNING" in line:
                parts = [p.strip() for p in line.split(":")]
                if len(parts) >= 3:
                    names.append(parts[-1].split("(")[0].strip())
    except Exception as e:
        print(f"  [WARN] flink list failed: {e}")
        return False

    found = " | ".join(names) if names else "(none)"
    print(f"  Found: {found}")
    needed = {"bronze": False, "silver": False, "gold": False}
    for name in names:
        for key in needed:
            if key in name.lower():
                needed[key] = True
    missing = [k for k, v in needed.items() if not v]
    if missing:
        print(f"  ✗ Missing: {missing}")
        return False
    print("  ✓ All 3 streaming jobs running.")
    return True


# ── Producer ──────────────────────────────────────────────────────────────────
def start_producer(users_per_tick: int) -> subprocess.Popen:
    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
        "HOURLY_CSV_PATH": "./hourly_fitbit_sema_df_unprocessed.csv",
        "DAILY_CSV_PATH":  "./daily_fitbit_sema_df_unprocessed.csv",
        "USERS_PER_TICK":  str(users_per_tick),
        "DELAY":           str(DELAY),
        "MAX_TICKS":       str(MAX_TICKS),
        "KAFKA_PARTITIONS": KAFKA_PARTITIONS,
    })
    python = ".venv/bin/python" if os.path.exists(".venv/bin/python") else "python3"
    proc   = subprocess.Popen(
        [python, "ingestion/producer_realtime.py"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    print(f"  [producer] PID={proc.pid}  users_per_tick={users_per_tick}  delay={DELAY}s")
    return proc


def req_per_sec(users_per_tick: int) -> float:
    return round(users_per_tick * 3 / DELAY, 1)


# ── Single run ────────────────────────────────────────────────────────────────
def run_once(
    users_per_tick: int,
    run_idx: int,
    is_warmup: bool,
    all_staleness: list[dict],
) -> dict:
    rps       = req_per_sec(users_per_tick)
    run_label = f"B_rate{rps}_run{run_idx}"
    tag       = " (WARMUP)" if is_warmup else ""
    print(f"\n{'='*70}")
    print(f"[B] Run {run_idx}  ≈{rps} req/s{tag}")
    print(f"{'='*70}")

    g_ns, g_tbl = GOLD_WATCH

    # Baselines
    base_b_ts   = latest_snapshot_ts_many(BRONZE_WATCH_TABLES)
    base_s_ts   = latest_snapshot_ts_many(SILVER_WATCH_TABLES)
    base_g_ts   = latest_snapshot_ts(g_ns, g_tbl)
    base_b_rows_by_table = snapshot_row_count_by_table(BRONZE_WATCH_TABLES)
    base_s_rows_by_table = snapshot_row_count_by_table(SILVER_WATCH_TABLES)
    base_b_rows = sum(base_b_rows_by_table.values())
    base_s_rows = sum(base_s_rows_by_table.values())
    base_g_rows = snapshot_row_count(g_ns, g_tbl)
    print(f"  [baseline] bronze={base_b_rows} silver={base_s_rows} gold={base_g_rows} rows")

    staleness_mon = StalenessMonitor(g_ns, g_tbl, run_label)

    # first_gold_wall is populated after Silver stabilises (see below).
    _first_gold_wall: list[float] = []

    with MemorySampler() as mem, staleness_mon:
        t0 = time.time()

        # ── Producer burst ────────────────────────────────────────────────────
        expected_rows = users_per_tick * len(INTRADAY_TABLES) * MAX_TICKS
        producer = start_producer(users_per_tick)
        print(f"  [producer] Running fixed workload: {MAX_TICKS} ticks …")
        try:
            producer.wait(timeout=PRODUCER_TIMEOUT_SECS)
        except subprocess.TimeoutExpired:
            producer.terminate()
            producer.wait(timeout=30)
            raise TimeoutError(
                f"Producer did not finish {MAX_TICKS} ticks within {PRODUCER_TIMEOUT_SECS}s"
            )
        t_prod_stop = round(time.time() - t0, 2)
        print(f"  [producer] Stopped at {t_prod_stop}s — waiting for pipeline to catch up …")

        # ── Wait for each layer to stabilise ─────────────────────────────────
        wait_results: dict[str, tuple[float, bool]] = {}

        def wait_layer(name: str, fn) -> None:
            *_, ok = fn()
            wait_results[name] = (round(time.time() - t0, 2), ok)

        # Gold commit tracker: runs from t0, records wall time of every Gold
        # Iceberg snapshot so we can derive t_gold without waiting for a NEW
        # snapshot AFTER Silver stabilises. Flink's Iceberg sink stops creating
        # snapshots once it has no in-flight data, so blocking on a post-Silver
        # snapshot is fragile. Mirrors Pipeline A's tracker approach for
        # consistent cross-pipeline measurement methodology.
        gold_last_commit_wall: list[float] = []  # updated on each new Gold snapshot
        gold_tracker_stop = threading.Event()

        def _track_gold_commits() -> None:
            last_ts = base_g_ts
            while not gold_tracker_stop.is_set():
                ts = latest_snapshot_ts(g_ns, g_tbl)
                if ts > last_ts:
                    wall_offset = round(time.time() - t0, 2)
                    last_ts = ts
                    if not _first_gold_wall:
                        _first_gold_wall.append(wall_offset)
                    gold_last_commit_wall.clear()
                    gold_last_commit_wall.append(wall_offset)
                gold_tracker_stop.wait(STABLE_POLL_SECS)

        gold_tracker = threading.Thread(target=_track_gold_commits, daemon=True)
        gold_tracker.start()

        # Bronze and Silver run in parallel — they are independent ingest paths.
        bs_threads = [
            threading.Thread(
                target=wait_layer,
                args=(
                    "bronze",
                    lambda: wait_for_stable_many(
                        BRONZE_WATCH_TABLES,
                        base_b_ts,
                        base_b_rows_by_table,
                        "Bronze",
                        expected_rows,
                    ),
                ),
                daemon=True,
            ),
            threading.Thread(
                target=wait_layer,
                args=(
                    "silver",
                    lambda: wait_for_stable_many(
                        SILVER_WATCH_TABLES,
                        base_s_ts,
                        base_s_rows_by_table,
                        "Silver",
                        expected_rows,
                    ),
                ),
                daemon=True,
            ),
        ]
        for t in bs_threads:
            t.start()
        for t in bs_threads:
            t.join()

        # After Silver is stable, wait for Gold to drain. Flink Iceberg sink
        # commits on each checkpoint that has data, so Gold may keep committing
        # briefly as in-flight pipelined data drains. Adaptive wait: poll until
        # Gold has been idle (no new snapshot) for `gold_idle_secs`, capped at
        # STABLE_MAX_WAIT total. Mirrors Pipeline A's adaptive settle.
        gold_idle_secs = GOLD_CHECKPOINT_SECONDS * 2 + 15
        gold_max_wait = STABLE_MAX_WAIT
        gold_wait_start = time.time()
        print(
            f"  [gold] Silver stable; waiting for Gold to drain "
            f"(idle={gold_idle_secs}s, max={gold_max_wait}s) …"
        )
        last_seen_wall = gold_last_commit_wall[0] if gold_last_commit_wall else -1.0
        last_change_time = time.time()
        while time.time() - gold_wait_start < gold_max_wait:
            current_wall = gold_last_commit_wall[0] if gold_last_commit_wall else -1.0
            if current_wall != last_seen_wall:
                last_seen_wall = current_wall
                last_change_time = time.time()
            elif time.time() - last_change_time >= gold_idle_secs:
                break
            time.sleep(STABLE_POLL_SECS)
        gold_tracker_stop.set()
        gold_tracker.join(timeout=STABLE_POLL_SECS + 2)

        if gold_last_commit_wall:
            t_gold_val = gold_last_commit_wall[0]
            gold_ok = True
            print(f"    [stable] Gold → {t_gold_val}s")
        else:
            t_gold_val = round(time.time() - t0, 2)
            gold_ok = False
            print(f"    [TIMEOUT] Gold did not commit during this run")

        wait_results["gold"] = (t_gold_val, gold_ok)

        t_bronze, bronze_ok = wait_results.get("bronze", (round(time.time() - t0, 2), False))
        t_silver, silver_ok = wait_results.get("silver", (round(time.time() - t0, 2), False))
        t_gold, gold_ok = wait_results.get("gold", (round(time.time() - t0, 2), False))
        t_pipeline = max(t_bronze, t_silver, t_gold)

    # ── Row deltas ────────────────────────────────────────────────────────────
    rows_b = snapshot_row_count_many(BRONZE_WATCH_TABLES) - base_b_rows
    rows_s = snapshot_row_count_many(SILVER_WATCH_TABLES) - base_s_rows
    rows_g = snapshot_row_count(g_ns, g_tbl) - base_g_rows
    bronze_expected_ok = rows_b == expected_rows
    silver_expected_ok = rows_s == expected_rows
    bronze_ok = bronze_ok and bronze_expected_ok
    silver_ok = silver_ok and silver_expected_ok

    # ── Post-producer-stop catch-up lags ──────────────────────────────────────
    bronze_lag = round(max(0.0, t_bronze - t_prod_stop), 2)
    silver_lag = round(max(0.0, t_silver - t_prod_stop), 2)
    gold_lag   = round(max(0.0, t_gold   - t_prod_stop), 2)
    pipeline_lag = round(max(0.0, t_pipeline - t_prod_stop), 2)

    # ── First-message latency (producer start → first Gold commit) ────────────
    first_gold_latency = _first_gold_wall[0] if _first_gold_wall else -1.0

    # ── Throughput: rows committed per second of producer burst ───────────────
    bronze_tps = round(rows_b / WARMUP_SECS, 1) if rows_b > 0 else -1.0
    silver_tps = round(rows_s / WARMUP_SECS, 1) if rows_s > 0 else -1.0
    # Gold is an aggregate table. Depending on the sink semantics, its row
    # count may stay unchanged even when a new snapshot refreshes the output.
    gold_tps   = -1.0
    producer_actual_rps = round(rows_b / t_prod_stop, 1) if t_prod_stop > 0 and rows_b > 0 else -1.0
    processing_overhead_s = pipeline_lag
    catchup_ratio = round(pipeline_lag / WARMUP_SECS, 2) if WARMUP_SECS > 0 and pipeline_lag >= 0 else -1.0
    silver_to_bronze_ratio = round(rows_s / rows_b, 4) if rows_b > 0 else -1.0

    print(f"\n  ── Staleness (Gold during run) ──────────────────────────────")
    print(f"     avg={staleness_mon.avg_s}s  max={staleness_mon.max_s}s  min={staleness_mon.min_s}s")
    print(f"  ── First-message latency ────────────────────────────────────")
    print(f"     Gold first commit at {first_gold_latency}s after producer start")
    print(f"  ── Catch-up lag (after producer stop) ───────────────────────")
    print(f"     Bronze={bronze_lag}s  Silver={silver_lag}s  Gold={gold_lag}s")
    print(f"  ── Throughput (rows/s during burst) ─────────────────────────")
    print(f"     Bronze={bronze_tps}  Silver={silver_tps}")
    print(f"  ── Integrity metrics ────────────────────────────────────────")
    print(f"     actual_input={producer_actual_rps} rows/s  silver/bronze={silver_to_bronze_ratio}  catchup_ratio={catchup_ratio}x")
    print(f"  ── Gold refreshed: {'yes' if gold_ok else 'no'} rows_current={base_g_rows + rows_g} ──")
    if not bronze_expected_ok or not silver_expected_ok:
        print(
            "  ── Expected rows mismatch: "
            f"expected={expected_rows} bronze={rows_b} silver={rows_s} ──"
        )
    print(f"  ── E2E={t_pipeline}s  rows added: b={rows_b} s={rows_s} gold_delta={rows_g} ──")

    # ── Append staleness timeseries ───────────────────────────────────────────
    for sample in staleness_mon.samples:
        all_staleness.append({
            "pipeline":      "B",
            "run_label":     run_label,
            "users_per_tick": users_per_tick,
            "req_per_sec":   rps,
            "is_warmup":     is_warmup,
            **sample,
        })

    return {
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "pipeline":         "B",
        "run":              run_idx,
        "is_warmup":        is_warmup,
        "users_per_tick":   users_per_tick,
        "req_per_sec":      rps,
        "warmup_secs":      WARMUP_SECS,
        "target_ticks":     MAX_TICKS,
        "expected_rows":    expected_rows,
        # Staleness — KEY metric
        "avg_staleness_s":  staleness_mon.avg_s,
        "max_staleness_s":  staleness_mon.max_s,
        "min_staleness_s":  staleness_mon.min_s,
        # E2E
        "gold_e2e_s":       t_pipeline,
        "pipeline_e2e_s":   t_pipeline,
        "producer_stop_s":  t_prod_stop,
        "producer_actual_rps": producer_actual_rps,
        "processing_overhead_s": processing_overhead_s,
        "catchup_ratio": catchup_ratio,
        # Per-layer catch-up lags
        "bronze_lag_s":     bronze_lag,
        "silver_lag_s":     silver_lag,
        "gold_lag_s":       gold_lag,
        # Latency
        "first_gold_latency_s": first_gold_latency,
        # Throughput (rows committed / WARMUP_SECS)
        "rows_added_bronze":    rows_b,
        "rows_added_silver":    rows_s,
        "rows_added_gold":      rows_g,
        "silver_to_bronze_ratio": silver_to_bronze_ratio,
        "bronze_expected_ok":   bronze_expected_ok,
        "silver_expected_ok":   silver_expected_ok,
        "row_integrity_ok":     bronze_expected_ok and silver_expected_ok and silver_to_bronze_ratio == 1.0,
        "gold_rows_current":    base_g_rows + rows_g,
        "gold_refreshed":       gold_ok,
        "bronze_throughput_rps": bronze_tps,
        "silver_throughput_rps": silver_tps,
        "gold_throughput_rps":   gold_tps,
        # Status
        "bronze_ok":        bronze_ok,
        "silver_ok":        silver_ok,
        "gold_ok":          gold_ok,
        # Memory
        "engine_ram_mb":    mem.peak("flink-taskmanager"),
        "coord_ram_mb":     mem.peak("flink-jobmanager"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    if not verify_streaming_jobs():
        raise SystemExit(2)

    os.makedirs("benchmark", exist_ok=True)
    all_results:   list[dict] = []
    all_staleness: list[dict] = []

    print("\nPipeline B Benchmark — Flink streaming + Iceberg")
    print(f"Request rates : {REQUEST_RATES} users/tick")
    print(f"Warmup runs   : {WARMUP_RUNS}  Measured runs: {N_RUNS}")
    print(f"Burst duration: {WARMUP_SECS}s equivalent ({MAX_TICKS} fixed ticks)  Staleness poll: {STALENESS_POLL_SECS}s")
    print(f"Results  → {RESULTS_FILE}")
    print(f"Staleness → {STALENESS_FILE}")

    for users_per_tick in REQUEST_RATES:
        rate_measured: list[dict] = []
        for run_idx in range(1, WARMUP_RUNS + N_RUNS + 1):
            is_warmup = run_idx <= WARMUP_RUNS
            try:
                result = run_once(users_per_tick, run_idx, is_warmup, all_staleness)
                all_results.append(result)
                _save_csv(all_results, RESULTS_FILE)
                _save_csv(all_staleness, STALENESS_FILE)
                if not is_warmup:
                    rate_measured.append(result)
            except Exception as e:
                print(f"  [ERROR] Run {run_idx} failed: {e}")
                import traceback; traceback.print_exc()

        if rate_measured:
            _print_rate_summary(users_per_tick, rate_measured)

    _print_summary_table(all_results)
    print(f"\nDone. Results → {RESULTS_FILE}  Staleness → {STALENESS_FILE}")


# ── Output helpers ────────────────────────────────────────────────────────────
def _save_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def _m(results: list[dict], key: str) -> float:
    vals = [r[key] for r in results if r.get(key, -1) >= 0]
    return mean(vals) if vals else -1.0


def _print_rate_summary(users_per_tick: int, results: list[dict]) -> None:
    print(f"\n--- Rate {users_per_tick} users/tick ({req_per_sec(users_per_tick)} req/s) ---")
    print(f"  Gold E2E           : {_m(results, 'gold_e2e_s'):.1f}s")
    print(f"  First-msg latency  : {_m(results, 'first_gold_latency_s'):.1f}s  (producer start → first Gold commit)")
    print(f"  Avg staleness      : {_m(results, 'avg_staleness_s'):.1f}s")
    print(f"  Max staleness      : {_m(results, 'max_staleness_s'):.1f}s")
    print(f"  Bronze catch-up    : {_m(results, 'bronze_lag_s'):.1f}s")
    print(f"  Silver catch-up    : {_m(results, 'silver_lag_s'):.1f}s")
    print(f"  Gold catch-up      : {_m(results, 'gold_lag_s'):.1f}s")
    print(f"  Actual input rps   : {_m(results, 'producer_actual_rps'):.1f} rows/s")
    print(f"  Catch-up ratio     : {_m(results, 'catchup_ratio'):.2f}x")
    print(f"  Silver/Bronze      : {_m(results, 'silver_to_bronze_ratio'):.3f}")
    print(f"  Bronze throughput  : {_m(results, 'bronze_throughput_rps'):.1f} rows/s")
    print(f"  Silver throughput  : {_m(results, 'silver_throughput_rps'):.1f} rows/s")
    print(f"  Rows added         : bronze={_m(results, 'rows_added_bronze'):.0f} silver={_m(results, 'rows_added_silver'):.0f}")
    print(f"  Gold refreshed     : {sum(1 for r in results if r.get('gold_refreshed'))}/{len(results)} run(s)")
    print(f"  Engine RAM peak    : {_m(results, 'engine_ram_mb'):.0f} MiB")


def _print_summary_table(results: list[dict]) -> None:
    measured = [r for r in results if not r.get("is_warmup")]
    print("\n" + "=" * 120)
    print(f"{'req/s':>8} | {'E2E':>8} | {'1stLatency':>11} | {'AvgStale':>9} | {'MaxStale':>9} | "
          f"{'GoldLag':>8} | {'B rows/s':>8} | {'S rows/s':>8} | {'B rows':>7} | {'S rows':>7} | "
          f"{'GoldOK':>6} | {'EngineRAM':>10}")
    print("-" * 120)
    by_rate: dict[float, list] = {}
    for r in measured:
        by_rate.setdefault(r["req_per_sec"], []).append(r)
    for rate in sorted(by_rate):
        rs = by_rate[rate]
        def f(k): return f"{_m(rs,k):.1f}s"
        def fmb(k):
            v = [r[k] for r in rs if r.get(k,-1)>0]
            return f"{mean(v):.0f}MiB" if v else "N/A"
        gold_ok = f"{sum(1 for r in rs if r.get('gold_refreshed'))}/{len(rs)}"
        bronze_tps = f"{_m(rs, 'bronze_throughput_rps'):.1f}"
        silver_tps = f"{_m(rs, 'silver_throughput_rps'):.1f}"
        bronze_rows = f"{_m(rs, 'rows_added_bronze'):.0f}"
        silver_rows = f"{_m(rs, 'rows_added_silver'):.0f}"
        print(f"{rate:>8} | {f('gold_e2e_s'):>8} | {f('first_gold_latency_s'):>11} | "
              f"{f('avg_staleness_s'):>9} | {f('max_staleness_s'):>9} | "
              f"{f('gold_lag_s'):>8} | {bronze_tps:>8} | {silver_tps:>8} | "
              f"{bronze_rows:>7} | {silver_rows:>7} | {gold_ok:>6} | {fmb('engine_ram_mb'):>10}")


if __name__ == "__main__":
    main()
