"""
Pipeline A - Spark Structured Streaming benchmark.

Bronze, Silver, and Gold are expected to run continuously:
  Kafka -> Bronze Delta -> Silver Delta -> Gold Delta

The benchmark emits intraday events, then observes Delta commit timestamps and
row deltas to measure freshness, catch-up lag, throughput, and memory.
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


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
NAMENODE_CONTAINER = os.getenv("NAMENODE_CONTAINER", "pipeline_a-namenode-1")
N_RUNS = int(os.getenv("N_RUNS", "3"))
WARMUP_RUNS = int(os.getenv("WARMUP_RUNS", "1"))
WARMUP_SECS = int(os.getenv("WARMUP_SECS", "10"))
DELAY = float(os.getenv("DELAY", "0.1"))
MEM_SAMPLE_SECS = float(os.getenv("MEM_SAMPLE_SECS", "2.0"))
STALENESS_POLL_SECS = float(os.getenv("STALENESS_POLL_SECS", "5.0"))
STABLE_POLL_SECS = float(os.getenv("STABLE_POLL_SECS", "3.0"))
STABLE_POLLS_REQUIRED = int(os.getenv("STABLE_POLLS_REQUIRED", "3"))
STABLE_MAX_WAIT = int(os.getenv("STABLE_MAX_WAIT", "800"))
# Gold trigger interval (seconds). Used to size the post-Silver settle window so
# Gold has time to process Silver's final micro-batch before we record t_gold.
GOLD_TRIGGER_SECONDS = int(os.getenv("GOLD_TRIGGER_SECONDS", "30"))

REQUEST_RATES = [int(x.strip()) for x in os.getenv("REQUEST_RATES", "50,100,200").split(",")]
MAX_TICKS = int(os.getenv("MAX_TICKS", str(max(1, round(WARMUP_SECS / DELAY)))))
PRODUCER_TIMEOUT_SECS = int(os.getenv("PRODUCER_TIMEOUT_SECS", str(max(60, int(WARMUP_SECS * 6)))))

HDFS_BRONZE = "hdfs://namenode:9000/data/bronze/wearable"
HDFS_SILVER = "hdfs://namenode:9000/data/silver/wearable"
HDFS_GOLD = "hdfs://namenode:9000/data/gold/wearable"

INTRADAY_TABLES = ("heart_rate_intraday", "hrv_intraday", "breathing_intraday")
BRONZE_WATCH_TABLES = [f"{HDFS_BRONZE}/{table}" for table in INTRADAY_TABLES]
SILVER_WATCH_TABLES = [f"{HDFS_SILVER}/{table}" for table in INTRADAY_TABLES]
GOLD_WATCH = f"{HDFS_GOLD}/daily_intraday_summary"

# Historical full-pipeline Bronze table names. The benchmark workload is scoped
# to the three intraday tables below.
_BRONZE_NON_INTRADAY = (
    "vitals", "activity", "context", "profile",
    "sleep", "hrv_summary", "breathing_summary", "vitals_daily",
)
BRONZE_BACKGROUND_TABLES = [f"{HDFS_BRONZE}/{t}" for t in _BRONZE_NON_INTRADAY]

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_FILE = f"benchmark/results_{_ts}.csv"
STALENESS_FILE = f"benchmark/staleness_{_ts}.csv"


def _hdfs(args: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", NAMENODE_CONTAINER, "hdfs", "dfs", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


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
    logs = delta_json_logs(table_path)
    if not logs:
        return 0
    # Scan from the latest log file backwards. Spark Structured Streaming writes
    # a txn-only commit (0-byte or commitInfo-less) at the start of each trigger
    # before actual data arrives, leaving an empty tail file. Reading only the
    # latest would return 0, collapsing all cache keys to 0 and preventing the
    # row-count cache from ever being reused. Scanning backward until we find a
    # file with a real timestamp avoids this.
    for log in reversed(logs):
        best = 0
        for action in _read_json_lines(log):
            if "commitInfo" in action:
                best = max(best, int(action["commitInfo"].get("timestamp", 0) or 0))
            if "add" in action:
                best = max(best, int(action["add"].get("modificationTime", 0) or 0))
        if best > 0:
            return best
    return 0


# Cache: table_path -> (latest_commit_ts_ms, row_count)
# Avoids re-reading all Delta log JSON files when the table hasn't changed.
# Without this, each poll call is O(n log files); with it, O(1) on stable tables.
_delta_count_cache: dict[str, tuple[int, int]] = {}


def delta_row_count(table_path: str) -> int:
    # IMPORTANT: call delta_json_logs exactly once and read each file exactly
    # once.  A previous design called delta_json_logs twice — once here and
    # once inside latest_delta_ts_ms — which created a TOCTOU race: Spark
    # could write a new log file (e.g. f2 with data) between the two calls,
    # so logs=[f0,f1] but latest_ts came from f2.  The cache stored (T2, 0)
    # and every subsequent call hit the cache and returned 0 forever.
    logs = delta_json_logs(table_path)
    if not logs:
        return 0

    # Read every log file once; reuse the parsed actions for both the
    # timestamp extraction and the row count calculation.
    all_actions: dict[str, list[dict]] = {log: _read_json_lines(log) for log in logs}

    # Derive latest_ts from the same snapshot of files (backward scan so
    # empty / partial tail files are skipped — see latest_delta_ts_ms).
    latest_ts = 0
    for log in reversed(logs):
        best = 0
        for action in all_actions[log]:
            if "commitInfo" in action:
                best = max(best, int(action["commitInfo"].get("timestamp", 0) or 0))
            if "add" in action:
                best = max(best, int(action["add"].get("modificationTime", 0) or 0))
        if best > 0:
            latest_ts = best
            break

    cached = _delta_count_cache.get(table_path)
    if cached and cached[0] == latest_ts and latest_ts > 0:
        return cached[1]

    active: dict[str, int] = {}
    for log in logs:
        for action in all_actions[log]:
            if "add" in action:
                add = action["add"]
                stats = add.get("stats", "{}")
                try:
                    stats_obj = json.loads(stats) if isinstance(stats, str) else (stats or {})
                except json.JSONDecodeError:
                    stats_obj = {}
                active[add["path"]] = int((stats_obj or {}).get("numRecords", 0) or 0)
            elif "remove" in action:
                active.pop(action["remove"].get("path"), None)
    count = sum(active.values())
    _delta_count_cache[table_path] = (latest_ts, count)
    return count


def delta_row_count_many(table_paths: list[str]) -> int:
    return sum(delta_row_count(path) for path in table_paths)


def latest_delta_ts_many(table_paths: list[str]) -> dict[str, int]:
    return {path: latest_delta_ts_ms(path) for path in table_paths}


def delta_row_count_by_table(table_paths: list[str]) -> dict[str, int]:
    return {path: delta_row_count(path) for path in table_paths}


def wait_for_stable(
    table_path: str,
    baseline_ts: int,
    label: str,
    baseline_rows: int | None = None,
) -> tuple[float, int, bool]:
    start = time.time()
    last_ts = 0
    last_rows: int | None = None
    stable_n = 0
    while time.time() - start < STABLE_MAX_WAIT:
        ts = latest_delta_ts_ms(table_path)
        rows = delta_row_count(table_path) if baseline_rows is not None else None
        has_new_commit = ts > baseline_ts
        has_new_rows = rows is None or rows > baseline_rows
        stable_value = rows if rows is not None else ts
        if has_new_commit and has_new_rows:
            if stable_value == (last_rows if rows is not None else last_ts):
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    waited = round(time.time() - start, 2)
                    print(f"    [stable] {label} -> {waited}s")
                    return waited, ts, True
            else:
                last_ts = ts
                last_rows = rows
                stable_n = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label} did not stabilise within {STABLE_MAX_WAIT}s")
    return float(STABLE_MAX_WAIT), last_ts, False


def wait_for_stable_many(
    table_paths: list[str],
    baseline_ts: dict[str, int],
    baseline_rows: dict[str, int],
    label: str,
    expected_delta_rows: int | None = None,
) -> tuple[float, int, bool]:
    start = time.time()
    baseline_total_rows = sum(baseline_rows.values())
    last_total_rows: int | None = None
    last_min_ts = 0
    stable_n = 0
    _last_debug_elapsed = -999.0  # throttle per-poll debug lines to every 60s
    while time.time() - start < STABLE_MAX_WAIT:
        current_ts = latest_delta_ts_many(table_paths)
        current_rows = delta_row_count_by_table(table_paths)
        all_new_commits = all(current_ts[path] > baseline_ts[path] for path in table_paths)
        all_new_rows = all(current_rows[path] > baseline_rows[path] for path in table_paths)
        total_rows = sum(current_rows.values())
        # enough_rows: the total row delta meets or exceeds the expected burst
        # size. We intentionally do NOT require a per-table minimum here — if
        # one Bronze table's Delta log lacks row statistics (numRecords missing
        # or 0) while the other tables are fine, the per-table check would
        # block indefinitely even though all expected data is present across
        # the three tables combined.
        enough_rows = (
            expected_delta_rows is None
            or total_rows - baseline_total_rows >= expected_delta_rows
        )
        # Gate: proceed to stability counting when enough rows are present, OR
        # when every table has at least one new commit and row (for the case
        # where expected_delta_rows is None, e.g. Gold).
        # Do NOT require all_new_commits when enough_rows is already True —
        # on a cold Kafka consumer one partition can lag, leaving one table
        # without its first commit for hundreds of seconds even after the
        # other tables have all their data.
        ready_to_count = enough_rows or (all_new_commits and all_new_rows)
        elapsed = round(time.time() - start, 1)
        if not ready_to_count and elapsed - _last_debug_elapsed >= 60:
            _last_debug_elapsed = elapsed
            per_tbl = {
                p.split("/")[-1]: (
                    current_rows[p] - baseline_rows[p],
                    "commit" if current_ts[p] > baseline_ts[p] else "no-commit",
                )
                for p in table_paths
            }
            print(f"    [{label}] waiting ({elapsed}s): {per_tbl}")
        if ready_to_count:
            if total_rows == last_total_rows:
                stable_n += 1
                if stable_n >= STABLE_POLLS_REQUIRED:
                    waited = round(time.time() - start, 2)
                    final_ts = min(current_ts.values()) if current_ts else 0
                    actual_delta = total_rows - baseline_total_rows
                    if expected_delta_rows is not None and actual_delta < expected_delta_rows:
                        print(
                            f"    [WARN] {label} stable but only {actual_delta} rows "
                            f"(expected {expected_delta_rows}) — one table may still be lagging"
                        )
                    print(f"    [stable] {label} -> {waited}s")
                    return waited, final_ts, True
            else:
                last_total_rows = total_rows
                last_min_ts = min(current_ts.values()) if current_ts else 0
                stable_n = 0
        time.sleep(STABLE_POLL_SECS)
    print(f"    [TIMEOUT] {label} did not stabilise within {STABLE_MAX_WAIT}s")
    return float(STABLE_MAX_WAIT), last_min_ts, False


class StalenessMonitor:
    def __init__(self, table_path: str):
        self.table_path = table_path
        self.samples: list[dict] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._t0 = 0.0

    def __enter__(self) -> "StalenessMonitor":
        self._t0 = time.time()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        while not self._stop.is_set():
            ts_ms = latest_delta_ts_ms(self.table_path)
            if ts_ms > 0:
                self.samples.append({
                    "wall_s": round(time.time() - self._t0, 1),
                    "staleness_s": round(max(0.0, time.time() - ts_ms / 1000.0), 1),
                })
            self._stop.wait(STALENESS_POLL_SECS)

    def _vals(self) -> list[float]:
        return [s["staleness_s"] for s in self.samples]

    @property
    def avg_s(self) -> float:
        vals = self._vals()
        return round(mean(vals), 2) if vals else -1.0

    @property
    def max_s(self) -> float:
        vals = self._vals()
        return round(max(vals), 2) if vals else -1.0

    @property
    def min_s(self) -> float:
        vals = self._vals()
        return round(min(vals), 2) if vals else -1.0


def _docker_mem_mb() -> dict[str, float]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
        capture_output=True,
        text=True,
    )
    out: dict[str, float] = {}
    for line in result.stdout.splitlines():
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
        self._stop = threading.Event()
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
        return max((mb for name, mb in self._peaks.items() if substring in name), default=-1.0)


def verify_streaming_jobs() -> bool:
    print("[preflight] Verifying Spark streaming jobs are running ...")
    required = {
        "spark_bronze.py": False,
        "spark_silver_streaming.py": False,
        "spark_gold_streaming.py": False,
    }
    r = subprocess.run(["pgrep", "-af", "spark_"], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        for script in required:
            if script in line:
                required[script] = True
    for script, ok in required.items():
        print(f"  {'OK' if ok else 'MISSING'} {script}")
    return all(required.values())


def start_producer(users_per_tick: int) -> subprocess.Popen:
    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
        "HOURLY_CSV_PATH": "./hourly_fitbit_sema_df_unprocessed.csv",
        "DAILY_CSV_PATH": "./daily_fitbit_sema_df_unprocessed.csv",
        "USERS_PER_TICK": str(users_per_tick),
        "DELAY": str(DELAY),
        "MAX_TICKS": str(MAX_TICKS),
    })
    python = ".venv/bin/python" if os.path.exists(".venv/bin/python") else "python3"
    proc = subprocess.Popen([python, "ingestion/producer_realtime.py"], env=env)
    print(f"  [producer] PID={proc.pid} users_per_tick={users_per_tick} delay={DELAY}s")
    return proc


def req_per_sec(users_per_tick: int) -> float:
    return round(users_per_tick * 3 / DELAY, 1)


def run_once(users_per_tick: int, run_idx: int, is_warmup: bool, all_staleness: list[dict]) -> dict:
    rps = req_per_sec(users_per_tick)
    run_label = f"A_rate{rps}_run{run_idx}"
    tag = " (WARMUP)" if is_warmup else ""
    print(f"\n{'=' * 70}")
    print(f"[A] Run {run_idx} approx {rps} req/s{tag}")
    print(f"{'=' * 70}")

    base_b_ts = latest_delta_ts_many(BRONZE_WATCH_TABLES)
    base_s_ts = latest_delta_ts_many(SILVER_WATCH_TABLES)
    base_g_ts = latest_delta_ts_ms(GOLD_WATCH)
    base_b_rows_by_table = delta_row_count_by_table(BRONZE_WATCH_TABLES)
    base_s_rows_by_table = delta_row_count_by_table(SILVER_WATCH_TABLES)
    base_b_rows = sum(base_b_rows_by_table.values())
    base_s_rows = sum(base_s_rows_by_table.values())
    base_g_rows = delta_row_count(GOLD_WATCH)
    print(f"  [baseline] bronze={base_b_rows} silver={base_s_rows} gold={base_g_rows} rows")

    first_gold_wall: list[float] = []
    staleness_mon = StalenessMonitor(GOLD_WATCH)

    with MemorySampler() as mem, staleness_mon:
        t0 = time.time()

        expected_rows = users_per_tick * len(INTRADAY_TABLES) * MAX_TICKS
        producer = start_producer(users_per_tick)
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
        print(f"  [producer] Stopped at {t_prod_stop}s; waiting for Delta commits to stabilise ...")

        wait_results: dict[str, tuple[float, bool]] = {}

        def wait_layer(name: str, fn) -> None:
            *_, ok = fn()
            wait_results[name] = (round(time.time() - t0, 2), ok)

        # Gold commit tracker: runs from t0, records wall time of every Gold
        # Delta commit so we can derive t_gold without waiting for a NEW commit
        # after Silver. Gold uses readStream (incremental), so it emits no new
        # commits once Silver is stable — we must NOT block on a post-Silver commit.
        gold_last_commit_wall: list[float] = []  # updated on each new Gold commit
        gold_tracker_stop = threading.Event()

        def _track_gold_commits() -> None:
            last_ts = base_g_ts
            while not gold_tracker_stop.is_set():
                ts = latest_delta_ts_ms(GOLD_WATCH)
                if ts > last_ts:
                    wall_offset = round(time.time() - t0, 2)
                    last_ts = ts
                    if not first_gold_wall:
                        first_gold_wall.append(wall_offset)
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

        # After Silver is stable, wait for Gold to drain its backlog. Gold may
        # be many micro-batches behind Silver (Spark Structured Streaming with
        # foreachBatch is sequential — a slow refresh_gold queues triggers).
        # Adaptive wait: poll until Gold has been idle (no new commits) for
        # `gold_idle_secs`, capped at STABLE_MAX_WAIT total wait.
        gold_idle_secs = GOLD_TRIGGER_SECONDS * 2 + 15
        gold_max_wait = STABLE_MAX_WAIT
        gold_wait_start = time.time()
        print(
            f"  [gold] Silver stable; waiting for Gold to drain backlog "
            f"(idle={gold_idle_secs}s, max={gold_max_wait}s) ..."
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
            print(f"    [stable] Gold -> {t_gold_val}s")
        else:
            t_gold_val = round(time.time() - t0, 2)
            gold_ok = False
            print(f"    [TIMEOUT] Gold did not commit during this run")

        wait_results["gold"] = (t_gold_val, gold_ok)

        t_bronze, bronze_ok = wait_results.get("bronze", (round(time.time() - t0, 2), False))
        t_silver, silver_ok = wait_results.get("silver", (round(time.time() - t0, 2), False))
        t_gold, gold_ok = wait_results.get("gold", (round(time.time() - t0, 2), False))
        t_pipeline = max(t_bronze, t_silver, t_gold)

    rows_b = delta_row_count_many(BRONZE_WATCH_TABLES) - base_b_rows
    rows_s = delta_row_count_many(SILVER_WATCH_TABLES) - base_s_rows
    rows_g = delta_row_count(GOLD_WATCH) - base_g_rows
    bronze_expected_ok = rows_b == expected_rows
    silver_expected_ok = rows_s == expected_rows
    bronze_ok = bronze_ok and bronze_expected_ok
    silver_ok = silver_ok and silver_expected_ok

    bronze_lag = round(max(0.0, t_bronze - t_prod_stop), 2)
    silver_lag = round(max(0.0, t_silver - t_prod_stop), 2)
    gold_lag = round(max(0.0, t_gold - t_prod_stop), 2)
    pipeline_lag = round(max(0.0, t_pipeline - t_prod_stop), 2)
    first_gold_latency = first_gold_wall[0] if first_gold_wall else -1.0

    bronze_tps = round(rows_b / WARMUP_SECS, 1) if rows_b > 0 else -1.0
    silver_tps = round(rows_s / WARMUP_SECS, 1) if rows_s > 0 else -1.0
    # Gold is an upsert aggregate table. Its row count may stay unchanged
    # across a run even when aggregate values are refreshed successfully.
    gold_tps = -1.0
    producer_actual_rps = round(rows_b / t_prod_stop, 1) if t_prod_stop > 0 and rows_b > 0 else -1.0
    processing_overhead_s = pipeline_lag
    catchup_ratio = round(pipeline_lag / WARMUP_SECS, 2) if WARMUP_SECS > 0 and pipeline_lag >= 0 else -1.0
    silver_to_bronze_ratio = round(rows_s / rows_b, 4) if rows_b > 0 else -1.0

    print(f"\n  Staleness avg={staleness_mon.avg_s}s max={staleness_mon.max_s}s min={staleness_mon.min_s}s")
    print(f"  First Gold commit: {first_gold_latency}s")
    print(f"  Catch-up lag: Bronze={bronze_lag}s Silver={silver_lag}s Gold={gold_lag}s")
    print(f"  Throughput rows/s: Bronze={bronze_tps} Silver={silver_tps}")
    print(
        "  Integrity metrics: "
        f"actual_input={producer_actual_rps} rows/s "
        f"silver/bronze={silver_to_bronze_ratio} "
        f"catchup_ratio={catchup_ratio}x"
    )
    print(f"  Gold refreshed: {'yes' if gold_ok else 'no'} rows_current={base_g_rows + rows_g}")
    if not bronze_expected_ok or not silver_expected_ok:
        print(
            "  [integrity] Expected rows mismatch: "
            f"expected={expected_rows} bronze={rows_b} silver={rows_s}"
        )
    print(f"  E2E={t_pipeline}s rows added: b={rows_b} s={rows_s} gold_delta={rows_g}")

    for sample in staleness_mon.samples:
        all_staleness.append({
            "pipeline": "A",
            "run_label": run_label,
            "users_per_tick": users_per_tick,
            "req_per_sec": rps,
            "is_warmup": is_warmup,
            **sample,
        })

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline": "A",
        "run": run_idx,
        "is_warmup": is_warmup,
        "users_per_tick": users_per_tick,
        "req_per_sec": rps,
        "warmup_secs": WARMUP_SECS,
        "target_ticks": MAX_TICKS,
        "expected_rows": expected_rows,
        "avg_staleness_s": staleness_mon.avg_s,
        "max_staleness_s": staleness_mon.max_s,
        "min_staleness_s": staleness_mon.min_s,
        "gold_e2e_s": t_pipeline,
        "pipeline_e2e_s": t_pipeline,
        "producer_stop_s": t_prod_stop,
        "producer_actual_rps": producer_actual_rps,
        "processing_overhead_s": processing_overhead_s,
        "catchup_ratio": catchup_ratio,
        "bronze_lag_s": bronze_lag,
        "silver_lag_s": silver_lag,
        "gold_lag_s": gold_lag,
        "first_gold_latency_s": first_gold_latency,
        "rows_added_bronze": rows_b,
        "rows_added_silver": rows_s,
        "rows_added_gold": rows_g,
        "silver_to_bronze_ratio": silver_to_bronze_ratio,
        "bronze_expected_ok": bronze_expected_ok,
        "silver_expected_ok": silver_expected_ok,
        "row_integrity_ok": bronze_expected_ok and silver_expected_ok and silver_to_bronze_ratio == 1.0,
        "gold_rows_current": base_g_rows + rows_g,
        "gold_refreshed": gold_ok,
        "bronze_throughput_rps": bronze_tps,
        "silver_throughput_rps": silver_tps,
        "gold_throughput_rps": gold_tps,
        "bronze_ok": bronze_ok,
        "silver_ok": silver_ok,
        "gold_ok": gold_ok,
        "engine_ram_mb": mem.peak("spark-worker"),
        "coord_ram_mb": mem.peak("spark-master"),
    }


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
    print(f"{'req/s':>8} | {'E2E':>8} | {'1stLatency':>11} | {'AvgStale':>9} | {'MaxStale':>9} | "
          f"{'GoldLag':>8} | {'B rows/s':>8} | {'S rows/s':>8} | {'B rows':>7} | {'S rows':>7} | "
          f"{'GoldOK':>6} | {'EngineRAM':>10}")
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

        gold_ok = f"{sum(1 for r in rows if r.get('gold_refreshed'))}/{len(rows)}"
        bronze_tps = f"{_m(rows, 'bronze_throughput_rps'):.1f}"
        silver_tps = f"{_m(rows, 'silver_throughput_rps'):.1f}"
        bronze_rows = f"{_m(rows, 'rows_added_bronze'):.0f}"
        silver_rows = f"{_m(rows, 'rows_added_silver'):.0f}"
        print(f"{rate:>8} | {sec('gold_e2e_s'):>8} | {sec('first_gold_latency_s'):>11} | "
              f"{sec('avg_staleness_s'):>9} | {sec('max_staleness_s'):>9} | "
              f"{sec('gold_lag_s'):>8} | {bronze_tps:>8} | {silver_tps:>8} | "
              f"{bronze_rows:>7} | {silver_rows:>7} | {gold_ok:>6} | {mb('engine_ram_mb'):>10}")


# Legacy compact script kept only for reference. Benchmark runs no longer call
# OPTIMIZE because it is a maintenance workload and distorted latency results.
# OPTIMIZE is safe — Delta handles concurrent writers via MVCC.
_COMPACT_SCRIPT_TEMPLATE = """\
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("BenchmarkCompact")
    .master("local[2]")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "512m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
tables = {tables!r}
for t in tables:
    try:
        print(f"[compact] OPTIMIZE {{t}} ...")
        spark.sql(f"OPTIMIZE delta.`{{t}}`")
        print(f"[compact] done.")
    except Exception as e:
        print(f"[compact] WARNING: {{e}}")
spark.stop()
"""


def compact_delta_tables() -> None:
    """Disabled: do not run Delta OPTIMIZE inside benchmark measurements."""
    return

    """Run OPTIMIZE on all watched Delta tables between consecutive runs.

    Called after every run except the very last one — both between runs within
    the same rate tier and between rate tiers. This prevents small Parquet files
    from each Spark micro-batch from accumulating across runs, which would
    cause Silver's streaming reads to slow down progressively and time out.

    Keeps measurement conditions consistent with Pipeline B, which uses the
    Iceberg REST catalog for O(1) metadata lookups regardless of history size.

    The row-count cache (_delta_count_cache) is also cleared so the next
    baseline measurement does a fresh full recount from scratch.
    """
    if SKIP_COMPACT:
        print("  [compact] Skipped (SKIP_COMPACT=1)")
        return

    tables = [
        *BRONZE_WATCH_TABLES,
        *BRONZE_BACKGROUND_TABLES,
        *SILVER_WATCH_TABLES,
        GOLD_WATCH,
    ]
    script = _COMPACT_SCRIPT_TEMPLATE.format(tables=tables)

    n_bronze = len(BRONZE_WATCH_TABLES) + len(BRONZE_BACKGROUND_TABLES)
    print(
        f"  [compact] Running OPTIMIZE on {len(tables)} Delta tables "
        f"({n_bronze} Bronze = 3 intraday + {len(BRONZE_BACKGROUND_TABLES)} background, "
        f"{len(SILVER_WATCH_TABLES)} Silver intraday, 1 Gold) ..."
    )
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, prefix="benchmark_compact_"
        ) as f:
            f.write(script)
            host_path = f.name

        container_path = "/tmp/benchmark_compact.py"
        subprocess.run(
            ["docker", "cp", host_path, f"{SPARK_MASTER_CONTAINER}:{container_path}"],
            check=True, capture_output=True,
        )
        proc = subprocess.run(
            [
                "docker", "exec", SPARK_MASTER_CONTAINER,
                "/opt/spark/bin/spark-submit",
                "--packages", "io.delta:delta-spark_2.12:3.2.0",
                container_path,
            ],
            capture_output=True, text=True, timeout=300,
        )
        if proc.returncode != 0:
            print(f"  [compact] WARNING: spark-submit exited {proc.returncode}")
            if proc.stderr:
                print(f"  [compact] stderr (last 500 chars): {proc.stderr[-500:]}")
        else:
            print("  [compact] Done.")
    except subprocess.TimeoutExpired:
        print("  [compact] WARNING: timed out after 300s, continuing anyway.")
    except Exception as exc:
        print(f"  [compact] WARNING: {exc}, continuing anyway.")
    finally:
        try:
            os.unlink(host_path)
        except Exception:
            pass

    _delta_count_cache.clear()
    print("  [compact] Row-count cache cleared.")


def main() -> None:
    if not verify_streaming_jobs():
        raise SystemExit("Missing Spark streaming jobs. Run benchmark/run_benchmark.sh to start them.")

    os.makedirs("benchmark", exist_ok=True)
    all_results: list[dict] = []
    all_staleness: list[dict] = []

    print("\nPipeline A Benchmark - Spark Structured Streaming + Delta Lake")
    print(f"Request rates : {REQUEST_RATES} users/tick")
    print(f"Warmup runs   : {WARMUP_RUNS}  Measured runs: {N_RUNS}")
    print(f"Burst duration: {WARMUP_SECS}s equivalent ({MAX_TICKS} fixed ticks)")
    print(f"Results       : {RESULTS_FILE}")
    print(f"Staleness     : {STALENESS_FILE}")

    total_runs_per_tier = WARMUP_RUNS + N_RUNS
    for rate_idx, users_per_tick in enumerate(REQUEST_RATES):
        measured: list[dict] = []
        for run_idx in range(1, total_runs_per_tier + 1):
            is_warmup = run_idx <= WARMUP_RUNS
            try:
                result = run_once(users_per_tick, run_idx, is_warmup, all_staleness)
                all_results.append(result)
                _save_csv(all_results, RESULTS_FILE)
                _save_csv(all_staleness, STALENESS_FILE)
                if not is_warmup:
                    measured.append(result)
            except Exception as exc:
                print(f"  [ERROR] Run {run_idx} failed: {exc}")
                import traceback
                traceback.print_exc()
            # Do not run Delta OPTIMIZE inside the benchmark loop. It is a
            # maintenance workload and can distort measured streaming latency.
        if measured:
            _print_rate_summary(users_per_tick, measured)

    _print_summary_table(all_results)
    print(f"\nDone. Results -> {RESULTS_FILE}  Staleness -> {STALENESS_FILE}")


if __name__ == "__main__":
    main()
