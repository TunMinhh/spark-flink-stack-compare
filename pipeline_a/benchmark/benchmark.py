"""
Pipeline A - Spark Structured Streaming benchmark.

Bronze, Silver, and Gold are expected to run continuously:
  Kafka -> Bronze Delta -> Silver Delta -> Gold Delta

The benchmark emits intraday events, then observes Delta commit timestamps and
row deltas to measure freshness, catch-up lag, throughput, and memory.

Measurement methodology (v3):
  Bronze/Silver catch-up lag counts *data-bearing commits only*.  Stabilisation
  for these layers uses latest_data_delta_ts_ms(), which returns the timestamp
  of the most recent commit that contains an `add` action (real data files).
  Empty end-of-trigger commits written by Spark Structured Streaming (a fresh
  commitInfo.timestamp but no `add` actions) are skipped entirely: once the last
  data commit has landed the layer is treated as settled, no matter how many
  empty commits follow.  Bronze/Silver lag therefore reflects the time of the
  last real data write — matching the Gold tracker — instead of table-settlement
  time.  Iceberg never emits empty snapshots, so Pipeline B is unaffected and
  the two pipelines become directly comparable.

  Gold still uses latest_delta_ts_ms() (data + add modificationTime); its
  commits are driven by genuine aggregation state changes, so its freshness and
  lag are unchanged.

  Both timestamp scans read at most 1-2 log files per table in the common case
  (scan backwards from the latest, stop at the first qualifying commit), O(1) in
  log-file count regardless of how many commits have accumulated.  Txn-only
  start-of-trigger commits (no commitInfo.timestamp, no add) carry no observable
  timestamp and are always skipped.

  Row counts are called exactly twice per run: once at baseline (before the
  producer starts) and once after the timestamp-based stabilisation fires.
  This removes the O(n) delta_row_count from the hot poll loop while keeping
  the same semantics as Pipeline B's Iceberg REST approach.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from statistics import mean


KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP",    "127.0.0.1:9092")
NAMENODE_CONTAINER = os.getenv("NAMENODE_CONTAINER", "pipeline_a-namenode-1")
N_RUNS             = int(os.getenv("N_RUNS",         "3"))
WARMUP_RUNS        = int(os.getenv("WARMUP_RUNS",    "1"))
WARMUP_SECS        = int(os.getenv("WARMUP_SECS",    "10"))
DELAY              = float(os.getenv("DELAY",        "0.1"))
MEM_SAMPLE_SECS    = float(os.getenv("MEM_SAMPLE_SECS",    "2.0"))
STALENESS_POLL_SECS   = float(os.getenv("STALENESS_POLL_SECS",   "5.0"))
STABLE_POLL_SECS      = float(os.getenv("STABLE_POLL_SECS",      "3.0"))
STABLE_POLLS_REQUIRED = int(os.getenv("STABLE_POLLS_REQUIRED",   "3"))
STABLE_MAX_WAIT       = int(os.getenv("STABLE_MAX_WAIT",          "800"))
GOLD_TRIGGER_SECONDS  = int(os.getenv("GOLD_TRIGGER_SECONDS",    "15"))

REQUEST_RATES = [int(x.strip()) for x in os.getenv("REQUEST_RATES", "50,100,200").split(",")]
MAX_TICKS     = int(os.getenv("MAX_TICKS", str(max(1, round(WARMUP_SECS / DELAY)))))
PRODUCER_TIMEOUT_SECS = int(os.getenv("PRODUCER_TIMEOUT_SECS", str(max(60, int(WARMUP_SECS * 6)))))

HDFS_BRONZE = "hdfs://namenode:9000/data/bronze/wearable"
HDFS_SILVER = "hdfs://namenode:9000/data/silver/wearable"
HDFS_GOLD   = "hdfs://namenode:9000/data/gold/wearable"

INTRADAY_TABLES    = ("heart_rate_intraday", "hrv_intraday", "breathing_intraday")
BRONZE_WATCH_TABLES = [f"{HDFS_BRONZE}/{table}" for table in INTRADAY_TABLES]
SILVER_WATCH_TABLES = [f"{HDFS_SILVER}/{table}" for table in INTRADAY_TABLES]
GOLD_WATCH = f"{HDFS_GOLD}/daily_intraday_summary"

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_FILE   = f"benchmark/results_{_ts}.csv"
STALENESS_FILE = f"benchmark/staleness_{_ts}.csv"


# ── Low-level HDFS helpers ────────────────────────────────────────────────────

def _hdfs(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs", *args],
        capture_output=True, text=True, timeout=timeout,
    )


# ── O(n) log-scan helpers — used only at baseline and final row-count check ──

def delta_json_logs(table_path: str) -> list[str]:
    r = _hdfs(["-ls", f"{table_path}/_delta_log/"])
    if r.returncode != 0:
        return []
    logs = [
        line.split()[-1]
        for line in r.stdout.splitlines()
        if line.strip().endswith(".json")
    ]
    return sorted(logs)


def _read_json_lines(path: str) -> list[dict]:
    r = _hdfs(["-cat", path], timeout=60)
    if r.returncode != 0:
        return []
    out = []
    for line in r.stdout.splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out


def latest_delta_ts_ms(table_path: str) -> int:
    """Return the latest commit timestamp-ms from the Delta log.

    Scans backwards from the newest log file and returns the timestamp of the
    most recent commit that has either a commitInfo.timestamp or an add-action
    modificationTime.  This intentionally includes empty end-of-trigger commits
    (commitInfo present, no add actions) written by Spark Structured Streaming
    between data-bearing triggers.

    Rationale: empty commits are real Delta table activity.  A downstream
    consumer polling the table timestamp cannot distinguish an empty commit from
    a data commit via the public API, so it must wait until the table is truly
    quiescent.  bronze_lag / silver_lag therefore measure *table settlement
    time* — the interval until Spark's streaming engine stops touching the
    table — which is the correct latency figure from a consumer's perspective.
    Flink+Iceberg never creates empty snapshots, so Iceberg tables settle
    immediately after the last data checkpoint; that architectural difference is
    what the comparison captures.

    Txn-only start-of-trigger commits (no commitInfo.timestamp, no add) are
    still skipped because they carry no observable timestamp.
    """
    MAX_TAIL_READS = 4   # normally stops at 1-2 files; cap for safety
    logs = delta_json_logs(table_path)
    if not logs:
        return 0
    for log in reversed(logs[-MAX_TAIL_READS:]):
        best = 0
        for action in _read_json_lines(log):
            if "commitInfo" in action:
                best = max(best, int(action["commitInfo"].get("timestamp", 0) or 0))
            if "add" in action:
                best = max(best, int(action["add"].get("modificationTime", 0) or 0))
        if best > 0:
            return best
    return 0


def latest_delta_ts_ms_many(table_paths: list[str]) -> dict[str, int]:
    return {p: latest_delta_ts_ms(p) for p in table_paths}


def latest_data_delta_ts_ms(table_path: str) -> int:
    """Return the commit timestamp of the most recent *data-bearing* commit.

    A data-bearing commit is a Delta log entry that contains at least one ``add``
    action — i.e. new data files were actually written.  Empty end-of-trigger
    commits produced by Spark Structured Streaming (a fresh ``commitInfo.timestamp``
    but **no** ``add`` actions) are skipped entirely.

    Rationale: from a catch-up-lag perspective an empty commit carries no new
    data, so the table should be considered *settled* the moment its last data
    commit lands — regardless of how many empty commits Spark writes afterwards.
    This makes Bronze/Silver lag measure the time of the last real data write
    (the same definition the Gold tracker already uses) instead of table-
    settlement time.  Iceberg never emits empty snapshots, so Pipeline B is
    unaffected and the two pipelines become directly comparable.

    Scans backwards from the newest log file and returns the timestamp of the
    first commit that contains an ``add`` action (``commitInfo.timestamp``
    preferred, ``add.modificationTime`` as fallback).  Returns 0 if none found.
    """
    # Tolerate a run's worth of trailing empty commits stacked on top of the
    # last data commit (≈1 empty commit / trigger).  Normally stops at file 1-2.
    MAX_TAIL_READS = 40
    logs = delta_json_logs(table_path)
    if not logs:
        return 0
    for log in reversed(logs[-MAX_TAIL_READS:]):
        actions   = _read_json_lines(log)
        has_add   = any("add" in a for a in actions)
        if not has_add:
            continue  # empty / metadata-only commit → not data, keep scanning back
        commit_ts = 0
        add_ts    = 0
        for action in actions:
            if "commitInfo" in action:
                commit_ts = max(commit_ts, int(action["commitInfo"].get("timestamp", 0) or 0))
            if "add" in action:
                add_ts = max(add_ts, int(action["add"].get("modificationTime", 0) or 0))
        ts = commit_ts or add_ts
        if ts > 0:
            return ts
    return 0


def latest_data_delta_ts_ms_many(table_paths: list[str]) -> dict[str, int]:
    return {p: latest_data_delta_ts_ms(p) for p in table_paths}


# Cache: table_path -> (last_scanned_log_idx, {parquet_path: row_count})
# Incremental: each call only reads log files with index > last_scanned_log_idx,
# reducing per-call cost from O(all log files) to O(new log files since last call).
_delta_file_map: dict[str, tuple[int, dict[str, int]]] = {}


def _log_file_idx(log_path: str) -> int:
    name = log_path.rsplit("/", 1)[-1]
    try:
        return int(name.replace(".json", ""))
    except ValueError:
        return -1


def delta_row_count(table_path: str) -> int:
    """Net row count from Delta log. O(new log files since last call)."""
    logs = delta_json_logs(table_path)
    if not logs:
        return 0
    cached = _delta_file_map.get(table_path)
    if cached:
        last_idx, file_map = cached
        new_logs = [l for l in logs if _log_file_idx(l) > last_idx]
        file_map = dict(file_map)
    else:
        new_logs = logs
        file_map = {}
    for log in new_logs:
        for action in _read_json_lines(log):
            if "add" in action:
                add = action["add"]
                stats = add.get("stats", "{}")
                try:
                    stats_obj = json.loads(stats) if isinstance(stats, str) else (stats or {})
                except json.JSONDecodeError:
                    stats_obj = {}
                file_map[add["path"]] = int((stats_obj or {}).get("numRecords", 0) or 0)
            elif "remove" in action:
                file_map.pop(action["remove"].get("path"), None)
    _delta_file_map[table_path] = (_log_file_idx(logs[-1]), file_map)
    return sum(file_map.values())


def delta_row_count_many(table_paths: list[str]) -> int:
    return sum(delta_row_count(path) for path in table_paths)


def delta_row_count_by_table(table_paths: list[str]) -> dict[str, int]:
    return {path: delta_row_count(path) for path in table_paths}


# ── Stabilisation helpers (timestamp-based, O(1-2 files) per poll) ───────────

def wait_for_stable(
    table_path: str,
    baseline_ts: int,
    label: str,
) -> tuple[float, int, bool]:
    """Block until a new commit timestamp appears and then stays unchanged
    for STABLE_POLLS_REQUIRED consecutive polls.

    Uses latest_delta_ts_ms() which counts any commit with a timestamp,
    including Spark's empty end-of-trigger commits.  The layer is declared
    stable only when Spark's streaming engine stops touching the Delta table
    entirely — the correct consumer-facing definition of settlement.
    Row-count verification is done by the caller after this returns.
    """
    start    = time.time()
    last_ts  = 0
    stable_n = 0
    while time.time() - start < STABLE_MAX_WAIT:
        ts = latest_delta_ts_ms(table_path)
        if ts > baseline_ts:
            if ts == last_ts:
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    waited = round(time.time() - start, 2)
                    print(f"    [stable] {label} → {waited}s")
                    return waited, ts, True
            else:
                last_ts  = ts
                stable_n = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label} did not stabilise within {STABLE_MAX_WAIT}s")
    return float(STABLE_MAX_WAIT), last_ts, False


def wait_for_stable_many(
    table_paths: list[str],
    baseline_ts: dict[str, int],
    label: str,
    t0: float,
    ts_fn=latest_delta_ts_ms_many,
) -> tuple[float, int, bool]:
    """Block until every table has a NEW commit (per ``ts_fn``) and all of them
    stop moving for STABLE_POLLS_REQUIRED consecutive polls, then return the
    wall-clock offset (relative to ``t0``) at which the most recent new commit
    was observed — not the time the confirmation window ended.

    For Bronze and Silver we pass ``ts_fn=latest_data_delta_ts_ms_many`` so that
    only *data-bearing* commits advance the timestamp.  Empty end-of-trigger
    commits carry no ``add`` actions and are therefore invisible here: once the
    last data commit has landed, the layer is treated as settled no matter how
    many empty commits Spark writes afterwards.  The returned settlement time
    thus reflects the last real data write — the same semantics the Gold
    tracker uses — instead of table-settlement time.  O(1-2 files) per table
    per poll in the common case.
    """
    start            = time.time()
    last_ts_map: dict[str, int] = {}
    last_commit_wall = round(start - t0, 2)   # fallback if no new commit is seen
    stable_n         = 0
    while time.time() - start < STABLE_MAX_WAIT:
        current = ts_fn(table_paths)
        all_new = all(current[p] > baseline_ts[p] for p in table_paths)
        if all_new:
            if current == last_ts_map:
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    print(f"    [stable] {label} → last data commit at {last_commit_wall}s")
                    return last_commit_wall, min(current.values()), True
            else:
                # A new (data) commit appeared — record when we observed it.
                last_ts_map      = dict(current)
                last_commit_wall = round(time.time() - t0, 2)
                stable_n         = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label} did not stabilise within {STABLE_MAX_WAIT}s")
    return last_commit_wall, (min(last_ts_map.values()) if last_ts_map else 0), False


# ── Staleness monitor (uses latest_delta_ts_ms — O(1-2 files) per sample) ───

class StalenessMonitor:
    def __init__(self, table_path: str):
        self.table_path = table_path
        self.samples: list[dict] = []
        self._stop   = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0     = 0.0

    def __enter__(self) -> "StalenessMonitor":
        self._t0     = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def set_measure_from(self, wall_s: float) -> None:
        """Trim stats to samples at or after wall_s (e.g. first Gold commit).
        All samples are still written to the CSV; only avg/max/min are trimmed."""
        self._measure_from: float = wall_s

    def _loop(self) -> None:
        while not self._stop.is_set():
            ts_ms = latest_delta_ts_ms(self.table_path)
            if ts_ms > 0:
                staleness = max(0.0, time.time() - ts_ms / 1000.0)
                self.samples.append({
                    "wall_s":      round(time.time() - self._t0, 1),
                    "staleness_s": round(staleness, 1),
                })
            self._stop.wait(STALENESS_POLL_SECS)

    def _vals(self) -> list[float]:
        mf = getattr(self, "_measure_from", None)
        src = self.samples if mf is None else [s for s in self.samples if s["wall_s"] >= mf]
        return [s["staleness_s"] for s in src]

    @property
    def avg_s(self) -> float:
        vals = self._vals(); return round(mean(vals), 2) if vals else -1.0

    @property
    def max_s(self) -> float:
        vals = self._vals(); return round(max(vals), 2) if vals else -1.0

    @property
    def min_s(self) -> float:
        vals = self._vals(); return round(min(vals), 2) if vals else -1.0


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
        name = parts[0].replace("pipeline_a-", "").replace("-1", "")
        used = parts[1].split("/")[0].strip()
        try:
            if "GiB" in used:
                mb = float(used.replace("GiB", "")) * 1024
            elif "MiB" in used:
                mb = float(used.replace("MiB", ""))
            elif "kB" in used:
                mb = float(used.replace("kB", "")) / 1024
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
            for name, mb in _docker_mem_mb().items():
                if mb > self._peaks.get(name, -1):
                    self._peaks[name] = mb
            self._stop.wait(self.interval)

    def peak(self, substring: str) -> float:
        return max(
            (mb for name, mb in self._peaks.items() if substring in name),
            default=-1.0,
        )


# ── Preflight check ───────────────────────────────────────────────────────────

def verify_streaming_jobs() -> bool:
    print("[preflight] Verifying Spark streaming jobs are running ...")
    required = {
        "spark_bronze.py":           False,
        "spark_silver_streaming.py": False,
        "spark_gold_streaming.py":   False,
    }
    r = subprocess.run(["pgrep", "-af", "spark_"], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        for script in required:
            if script in line:
                required[script] = True
    for script, ok in required.items():
        print(f"  {'OK' if ok else 'MISSING'} {script}")
    return all(required.values())


# ── Producer ──────────────────────────────────────────────────────────────────

def start_producer(users_per_tick: int) -> subprocess.Popen:
    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP":  KAFKA_BOOTSTRAP,
        "HOURLY_CSV_PATH":  "../data/hourly_fitbit_sema_df_unprocessed.csv",
        "DAILY_CSV_PATH":   "../data/daily_fitbit_sema_df_unprocessed.csv",
        "USERS_PER_TICK":   str(users_per_tick),
        "DELAY":            str(DELAY),
        "MAX_TICKS":        str(MAX_TICKS),
    })
    python = ".venv/bin/python" if os.path.exists(".venv/bin/python") else "python3"
    proc   = subprocess.Popen([python, "ingestion/producer_realtime.py"], env=env)
    print(f"  [producer] PID={proc.pid} users_per_tick={users_per_tick} delay={DELAY}s")
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
    run_label = f"A_rate{rps}_run{run_idx}"
    tag       = " (WARMUP)" if is_warmup else ""
    print(f"\n{'=' * 70}")
    print(f"[A] Run {run_idx}  ≈{rps} req/s{tag}")
    print(f"{'=' * 70}")

    # ── Baselines — O(1-2 files) timestamp + O(n) row count, each done once ──
    # Bronze/Silver track *data-bearing* commits only (empty end-of-trigger
    # commits are ignored), so use the data-only timestamp for their baselines.
    base_b_ts = latest_data_delta_ts_ms_many(BRONZE_WATCH_TABLES)
    base_s_ts = latest_data_delta_ts_ms_many(SILVER_WATCH_TABLES)
    base_g_ts = latest_delta_ts_ms(GOLD_WATCH)

    base_b_rows_by_table = delta_row_count_by_table(BRONZE_WATCH_TABLES)
    base_s_rows_by_table = delta_row_count_by_table(SILVER_WATCH_TABLES)
    base_b_rows          = sum(base_b_rows_by_table.values())
    base_s_rows          = sum(base_s_rows_by_table.values())
    base_g_rows          = delta_row_count(GOLD_WATCH)
    print(f"  [baseline] bronze={base_b_rows} silver={base_s_rows} gold={base_g_rows} rows")

    first_gold_wall: list[float] = []
    staleness_mon = StalenessMonitor(GOLD_WATCH)

    with MemorySampler() as mem, staleness_mon:
        t0 = time.time()

        expected_rows = users_per_tick * len(INTRADAY_TABLES) * MAX_TICKS
        producer      = start_producer(users_per_tick)
        print(f"  [producer] Running fixed workload: {MAX_TICKS} ticks ...")
        try:
            producer.wait(timeout=PRODUCER_TIMEOUT_SECS)
        except subprocess.TimeoutExpired:
            producer.terminate()
            producer.wait(timeout=30)
            raise TimeoutError(
                f"Producer did not finish {MAX_TICKS} ticks within {PRODUCER_TIMEOUT_SECS}s"
            )
        t_prod_stop = round(time.time() - t0, 2)
        print(
            f"  [producer] Stopped at {t_prod_stop}s — "
            "waiting for Delta commits to stabilise (tail-log polling) ..."
        )

        wait_results: dict[str, tuple[float, bool]] = {}

        def wait_layer(name: str, fn) -> None:
            commit_wall, _final_ts, ok = fn()
            wait_results[name] = (commit_wall, ok)

        # Gold commit tracker — O(1-2 files), skips empty txn-only commits
        gold_last_commit_wall: list[float] = []
        gold_tracker_stop = threading.Event()

        def _track_gold_commits() -> None:
            last_ts = base_g_ts
            while not gold_tracker_stop.is_set():
                ts = latest_delta_ts_ms(GOLD_WATCH)
                if ts > last_ts:
                    wall_offset = round(time.time() - t0, 2)
                    last_ts     = ts
                    if not first_gold_wall:
                        first_gold_wall.append(wall_offset)
                        staleness_mon.set_measure_from(wall_offset)
                    gold_last_commit_wall.clear()
                    gold_last_commit_wall.append(wall_offset)
                gold_tracker_stop.wait(STABLE_POLL_SECS)

        gold_tracker = threading.Thread(target=_track_gold_commits, daemon=True)
        gold_tracker.start()

        # Bronze and Silver run in parallel — independent ingest paths.
        # Both watch *data-bearing* commits only: empty end-of-trigger commits
        # are ignored, so the layer settles at its last real data write.
        bs_threads = [
            threading.Thread(
                target=wait_layer,
                args=(
                    "bronze",
                    lambda: wait_for_stable_many(
                        BRONZE_WATCH_TABLES, base_b_ts, "Bronze",
                        t0, latest_data_delta_ts_ms_many,
                    ),
                ),
                daemon=True,
            ),
            threading.Thread(
                target=wait_layer,
                args=(
                    "silver",
                    lambda: wait_for_stable_many(
                        SILVER_WATCH_TABLES, base_s_ts, "Silver",
                        t0, latest_data_delta_ts_ms_many,
                    ),
                ),
                daemon=True,
            ),
        ]
        for t in bs_threads: t.start()
        for t in bs_threads: t.join()

        # After Silver is stable, let Gold drain its backlog
        gold_idle_secs  = GOLD_TRIGGER_SECONDS * 2 + 15
        gold_max_wait   = STABLE_MAX_WAIT
        gold_wait_start = time.time()
        print(
            f"  [gold] Silver stable; waiting for Gold to drain backlog "
            f"(idle={gold_idle_secs}s, max={gold_max_wait}s) ..."
        )
        last_seen_wall  = gold_last_commit_wall[0] if gold_last_commit_wall else -1.0
        last_change_t   = time.time()
        while time.time() - gold_wait_start < gold_max_wait:
            cw = gold_last_commit_wall[0] if gold_last_commit_wall else -1.0
            if cw != last_seen_wall:
                last_seen_wall = cw
                last_change_t  = time.time()
            elif time.time() - last_change_t >= gold_idle_secs:
                break
            time.sleep(STABLE_POLL_SECS)
        gold_tracker_stop.set()
        gold_tracker.join(timeout=STABLE_POLL_SECS + 2)

        if gold_last_commit_wall:
            t_gold_val = gold_last_commit_wall[0]
            gold_ok    = True
            print(f"    [stable] Gold → {t_gold_val}s")
        else:
            t_gold_val = round(time.time() - t0, 2)
            gold_ok    = False
            print(f"    [TIMEOUT] Gold did not commit during this run")

        wait_results["gold"] = (t_gold_val, gold_ok)

        t_bronze, bronze_ok = wait_results.get("bronze", (round(time.time() - t0, 2), False))
        t_silver, silver_ok = wait_results.get("silver", (round(time.time() - t0, 2), False))
        t_gold,   gold_ok   = wait_results.get("gold",   (round(time.time() - t0, 2), False))
        t_pipeline = max(t_bronze, t_silver, t_gold)

    # ── Final row counts — O(n), called ONCE after stabilisation ─────────────
    print("  [rowcount] Reading final row counts (single pass) ...")
    rows_b = delta_row_count_many(BRONZE_WATCH_TABLES) - base_b_rows
    rows_s = delta_row_count_many(SILVER_WATCH_TABLES) - base_s_rows
    rows_g = delta_row_count(GOLD_WATCH)               - base_g_rows

    bronze_expected_ok = rows_b == expected_rows
    silver_expected_ok = rows_s == expected_rows
    bronze_ok          = bronze_ok and bronze_expected_ok
    silver_ok          = silver_ok and silver_expected_ok

    bronze_lag  = round(max(0.0, t_bronze - t_prod_stop), 2)
    silver_lag  = round(max(0.0, t_silver - t_prod_stop), 2)
    gold_lag    = round(max(0.0, t_gold   - t_prod_stop), 2)
    pipeline_lag = round(max(0.0, t_pipeline - t_prod_stop), 2)
    first_gold_latency = first_gold_wall[0] if first_gold_wall else -1.0

    bronze_tps = round(rows_b / WARMUP_SECS, 1) if rows_b > 0 else -1.0
    silver_tps = round(rows_s / WARMUP_SECS, 1) if rows_s > 0 else -1.0
    gold_tps   = -1.0
    producer_actual_rps     = round(rows_b / t_prod_stop, 1) if t_prod_stop > 0 and rows_b > 0 else -1.0
    catchup_ratio           = round(pipeline_lag / WARMUP_SECS, 2) if WARMUP_SECS > 0 and pipeline_lag >= 0 else -1.0
    silver_to_bronze_ratio  = round(rows_s / rows_b, 4) if rows_b > 0 else -1.0

    print(f"\n  Staleness (post-first-Gold) avg={staleness_mon.avg_s}s max={staleness_mon.max_s}s min={staleness_mon.min_s}s  [samples={len(staleness_mon.samples)} total, {len(staleness_mon._vals())} post-first-gold]")
    print(f"  First Gold commit: {first_gold_latency}s")
    print(f"  Catch-up lag: Bronze={bronze_lag}s Silver={silver_lag}s Gold={gold_lag}s")
    print(f"  Throughput rows/s: Bronze={bronze_tps} Silver={silver_tps}")
    print(
        f"  Integrity: actual_input={producer_actual_rps} rows/s "
        f"silver/bronze={silver_to_bronze_ratio} catchup_ratio={catchup_ratio}x"
    )
    print(f"  Gold refreshed: {'yes' if gold_ok else 'no'} rows_current={base_g_rows + rows_g}")
    if not bronze_expected_ok or not silver_expected_ok:
        print(
            f"  [integrity] Expected rows mismatch: "
            f"expected={expected_rows} bronze={rows_b} silver={rows_s}"
        )
    print(f"  E2E={t_pipeline}s rows added: b={rows_b} s={rows_s} gold_delta={rows_g}")

    for sample in staleness_mon.samples:
        all_staleness.append({
            "pipeline":       "A",
            "run_label":      run_label,
            "users_per_tick": users_per_tick,
            "req_per_sec":    rps,
            "is_warmup":      is_warmup,
            **sample,
        })

    return {
        "timestamp":             datetime.now(timezone.utc).isoformat(),
        "pipeline":              "A",
        "run":                   run_idx,
        "is_warmup":             is_warmup,
        "users_per_tick":        users_per_tick,
        "req_per_sec":           rps,
        "warmup_secs":           WARMUP_SECS,
        "target_ticks":          MAX_TICKS,
        "expected_rows":         expected_rows,
        "avg_staleness_s":       staleness_mon.avg_s,
        "max_staleness_s":       staleness_mon.max_s,
        "min_staleness_s":       staleness_mon.min_s,
        "gold_e2e_s":            t_pipeline,
        "pipeline_e2e_s":        t_pipeline,
        "producer_stop_s":       t_prod_stop,
        "producer_actual_rps":   producer_actual_rps,
        "processing_overhead_s": pipeline_lag,
        "catchup_ratio":         catchup_ratio,
        "bronze_lag_s":          bronze_lag,
        "silver_lag_s":          silver_lag,
        "gold_lag_s":            gold_lag,
        "first_gold_latency_s":  first_gold_latency,
        "rows_added_bronze":     rows_b,
        "rows_added_silver":     rows_s,
        "rows_added_gold":       rows_g,
        "silver_to_bronze_ratio": silver_to_bronze_ratio,
        "bronze_expected_ok":    bronze_expected_ok,
        "silver_expected_ok":    silver_expected_ok,
        "row_integrity_ok":      bronze_expected_ok and silver_expected_ok and silver_to_bronze_ratio == 1.0,
        "gold_rows_current":     base_g_rows + rows_g,
        "gold_refreshed":        gold_ok,
        "bronze_throughput_rps": bronze_tps,
        "silver_throughput_rps": silver_tps,
        "gold_throughput_rps":   gold_tps,
        "bronze_ok":             bronze_ok,
        "silver_ok":             silver_ok,
        "gold_ok":               gold_ok,
        "engine_ram_mb":         mem.peak("spark-worker"),
        "coord_ram_mb":          mem.peak("spark-master"),
    }


# ── Output helpers ────────────────────────────────────────────────────────────

def _save_csv(rows: list[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _m(results: list[dict], key: str) -> float:
    vals = [r[key] for r in results if r.get(key, -1) >= 0]
    return mean(vals) if vals else -1.0


def _print_rate_summary(users_per_tick: int, results: list[dict]) -> None:
    print(f"\n--- Rate {users_per_tick} users/tick ({req_per_sec(users_per_tick)} req/s) ---")
    print(f"  Gold E2E          : {_m(results, 'gold_e2e_s'):.1f}s")
    print(f"  First-msg latency : {_m(results, 'first_gold_latency_s'):.1f}s")
    print(f"  Avg staleness     : {_m(results, 'avg_staleness_s'):.1f}s")
    print(f"  Max staleness     : {_m(results, 'max_staleness_s'):.1f}s")
    print(f"  Gold catch-up     : {_m(results, 'gold_lag_s'):.1f}s")
    print(f"  Actual input rps  : {_m(results, 'producer_actual_rps'):.1f} rows/s")
    print(f"  Catch-up ratio    : {_m(results, 'catchup_ratio'):.2f}x")
    print(f"  Silver/Bronze     : {_m(results, 'silver_to_bronze_ratio'):.3f}")
    print(f"  Bronze throughput : {_m(results, 'bronze_throughput_rps'):.1f} rows/s")
    print(f"  Silver throughput : {_m(results, 'silver_throughput_rps'):.1f} rows/s")
    print(f"  Rows added        : bronze={_m(results, 'rows_added_bronze'):.0f} silver={_m(results, 'rows_added_silver'):.0f}")
    print(f"  Gold refreshed    : {sum(1 for r in results if r.get('gold_refreshed'))}/{len(results)} run(s)")
    print(f"  Engine RAM peak   : {_m(results, 'engine_ram_mb'):.0f} MiB")


def _print_summary_table(results: list[dict]) -> None:
    measured = [r for r in results if not r.get("is_warmup")]
    print("\n" + "=" * 120)
    print(
        f"{'req/s':>8} | {'E2E':>8} | {'1stLatency':>11} | {'AvgStale':>9} | "
        f"{'MaxStale':>9} | {'GoldLag':>8} | {'B rows/s':>8} | {'S rows/s':>8} | "
        f"{'B rows':>7} | {'S rows':>7} | {'GoldOK':>6} | {'EngineRAM':>10}"
    )
    print("-" * 120)
    by_rate: dict[float, list[dict]] = {}
    for row in measured:
        by_rate.setdefault(row["req_per_sec"], []).append(row)
    for rate in sorted(by_rate):
        rows = by_rate[rate]

        def sec(key: str) -> str:
            return f"{_m(rows, key):.1f}s"

        def mb(key: str) -> str:
            vals = [r[key] for r in rows if r.get(key, -1) > 0]
            return f"{mean(vals):.0f}MiB" if vals else "N/A"

        gold_ok    = f"{sum(1 for r in rows if r.get('gold_refreshed'))}/{len(rows)}"
        bronze_tps = f"{_m(rows, 'bronze_throughput_rps'):.1f}"
        silver_tps = f"{_m(rows, 'silver_throughput_rps'):.1f}"
        bronze_rows = f"{_m(rows, 'rows_added_bronze'):.0f}"
        silver_rows = f"{_m(rows, 'rows_added_silver'):.0f}"
        print(
            f"{rate:>8} | {sec('gold_e2e_s'):>8} | {sec('first_gold_latency_s'):>11} | "
            f"{sec('avg_staleness_s'):>9} | {sec('max_staleness_s'):>9} | "
            f"{sec('gold_lag_s'):>8} | {bronze_tps:>8} | {silver_tps:>8} | "
            f"{bronze_rows:>7} | {silver_rows:>7} | {gold_ok:>6} | {mb('engine_ram_mb'):>10}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not verify_streaming_jobs():
        raise SystemExit("Missing Spark streaming jobs. Start them before running the benchmark.")

    os.makedirs("benchmark", exist_ok=True)
    all_results:   list[dict] = []
    all_staleness: list[dict] = []

    print("\nPipeline A Benchmark — Spark Structured Streaming + Delta Lake")
    print(f"Request rates  : {REQUEST_RATES} users/tick")
    print(f"Warmup runs    : {WARMUP_RUNS}  Measured runs: {N_RUNS}")
    print(f"Burst duration : {WARMUP_SECS}s equivalent ({MAX_TICKS} fixed ticks)")
    print(f"Polling method : O(1-2 files) timestamp-based (reads tail of Delta log)")
    print(f"Results        : {RESULTS_FILE}")
    print(f"Staleness      : {STALENESS_FILE}")

    for users_per_tick in REQUEST_RATES:
        measured: list[dict] = []
        for run_idx in range(1, WARMUP_RUNS + N_RUNS + 1):
            is_warmup = run_idx <= WARMUP_RUNS
            try:
                result = run_once(users_per_tick, run_idx, is_warmup, all_staleness)
                all_results.append(result)
                _save_csv(all_results,   RESULTS_FILE)
                _save_csv(all_staleness, STALENESS_FILE)
                if not is_warmup:
                    measured.append(result)
            except Exception as exc:
                print(f"  [ERROR] Run {run_idx} failed: {exc}")
                import traceback; traceback.print_exc()
        if measured:
            _print_rate_summary(users_per_tick, measured)

    _print_summary_table(all_results)
    print(f"\nDone. Results → {RESULTS_FILE}  Staleness → {STALENESS_FILE}")


if __name__ == "__main__":
    main()
