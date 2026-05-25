# Pipeline A Benchmark Guide

Pipeline A is benchmarked against the same intraday workload as Pipeline B:

```text
producer_realtime
-> Kafka intraday topics
-> Spark Bronze Delta intraday tables
-> Spark Silver Delta intraday tables
-> Spark Gold daily_intraday_summary
```

The benchmark intentionally avoids the unrelated daily/profile/wellness tables so Pipeline A and Pipeline B measure the same realtime input path.
Bronze, Silver, and Gold are all long-running Spark Structured Streaming jobs.

## Default Scope

The Makefile defaults are:

```make
SHUFFLE_PARTITIONS ?= 18
BRONZE_TOPICS ?= wearable_heart_rate_intraday,wearable_hrv_intraday,wearable_breathing_intraday
SILVER_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
GOLD_ONLY ?= daily_intraday_summary
REQUEST_RATES ?= 5,10,15,20
N_RUNS ?= 3
WARMUP_RUNS ?= 1
WARMUP_SECS ?= 30
```

This profile targets a single 32-vCPU / 64-GB VM. Kafka topics use 6 partitions,
Spark uses 18 shuffle partitions, and Pipeline B uses `PARALLELISM=6`.

## Run Pipeline A Benchmark

Start the stack:

```bash
cd ~/data-pipeline-comparison/pipeline_a
git pull

docker compose up -d
make init
```

The benchmark runner starts `make bronze`, `make silver`, and `make gold` in the
background if they are not already running. In this mode, `make silver` runs
`spark_silver_streaming.py` and `make gold` runs `spark_gold_streaming.py`.

Smoke test:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
SHUFFLE_PARTITIONS=18 \
bash benchmark/run_benchmark.sh
```

Official benchmark:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5,10,15,20 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=30 \
SHUFFLE_PARTITIONS=18 \
bash benchmark/run_benchmark.sh
```

Delta OPTIMIZE is intentionally not run inside the benchmark loop. It is a
maintenance workload, not part of the streaming path being measured, and can
distort latency numbers when mixed into benchmark runs.

## Request Rate Mapping

With default `DELAY=0.1`:

| USERS_PER_TICK | Approx req/s |
|----------------|--------------|
| 5 | 150 |
| 10 | 300 |
| 15 | 450 |
| 20 | 600 |

## Output

Results are written to:

```text
benchmark/results_YYYYMMDD_HHMMSS.csv
benchmark/staleness_YYYYMMDD_HHMMSS.csv
benchmark/benchmark_YYYYMMDD_HHMMSS.log
```

Use the same report metrics as Pipeline B:

```text
req_per_sec
gold_e2e_s
first_gold_latency_s
avg_staleness_s
max_staleness_s
bronze_lag_s
silver_lag_s
gold_lag_s
silver_throughput_rps
gold_throughput_rps
engine_ram_mb
```

## Metric Semantics

| Metric | Meaning |
|--------|---------|
| `gold_e2e_s` | Producer start → wall-clock time of Gold Delta's **last** commit during the run. Derived from a real-time commit tracker, not a polling-based stability check. |
| `first_gold_latency_s` | Producer start → wall-clock time of the **first** Gold Delta commit after `base_g_ts` (Gold's state captured before the run). Captured by the same tracker thread. |
| `avg_staleness_s` | Average Gold row staleness sampled every `STALENESS_POLL_SECS` seconds during the run |
| `max_staleness_s` | Maximum Gold row staleness during the burst |
| `bronze_lag_s` | Time between producer stop and Bronze table stabilizing |
| `silver_lag_s` | Time between producer stop and Silver table stabilizing |
| `gold_lag_s` | Time between producer stop and Gold's last commit |
| `silver_throughput_rps` | Silver rows written per second during the burst |
| `gold_throughput_rps` | Gold rows written per second during the burst |
| `engine_ram_mb` | Peak Spark worker RSS sampled during the run |

### Full CSV Reference

`results_*.csv` has one row per benchmark run. Warmup rows are kept in the CSV
for auditability, but rate summaries exclude rows where `is_warmup=true`.

| Metric | Meaning |
|--------|---------|
| `timestamp` | UTC timestamp when the run result row was written. |
| `pipeline` | Pipeline id. Pipeline A is Spark Structured Streaming + Delta Lake. |
| `run` | Run number inside the current request-rate tier. Warmup runs start at 1. |
| `is_warmup` | `true` for warmup runs. These are not included in printed per-rate averages. |
| `users_per_tick` | Number of synthetic users emitted by `producer_realtime.py` per producer tick. |
| `req_per_sec` | Intended logical event rate: `users_per_tick * 3 / DELAY`, because each user tick emits 3 intraday records. |
| `warmup_secs` | Historical name for producer burst duration. With fixed ticks, it is the target burst duration equivalent. |
| `target_ticks` | Fixed number of producer ticks for the run. Default is 100. |
| `expected_rows` | Expected Bronze/Silver row delta for the run: `users_per_tick * 3 * target_ticks`. |
| `avg_staleness_s` | Average Gold staleness sampled during the run. Staleness is `now - latest Gold commit timestamp`. |
| `max_staleness_s` | Worst sampled Gold staleness during the run. Higher values mean Gold was serving older data. |
| `min_staleness_s` | Best sampled Gold staleness during the run. |
| `gold_e2e_s` | Producer start to full pipeline completion for the measured run. In the current method this equals `pipeline_e2e_s`. |
| `pipeline_e2e_s` | Producer start to the point where Bronze/Silver have drained and Gold has settled. This is the main end-to-end latency metric. |
| `producer_stop_s` | Seconds from producer start until the producer finishes emitting the fixed workload. |
| `producer_actual_rps` | Actual rows emitted per second, computed from `expected_rows / producer_stop_s`. This can be lower than `req_per_sec` if producer startup or IO adds overhead. |
| `processing_overhead_s` | Time after producer stop until the pipeline is considered complete: `pipeline_e2e_s - producer_stop_s`. |
| `catchup_ratio` | `processing_overhead_s / warmup_secs`. Values greater than 1 mean catch-up took longer than the input burst duration. |
| `bronze_lag_s` | Seconds after producer stop until watched Bronze Delta tables reach expected rows and stop changing. |
| `silver_lag_s` | Seconds after producer stop until watched Silver Delta tables reach expected rows and stop changing. |
| `gold_lag_s` | Seconds after producer stop until the last observed Gold Delta commit used for this run. |
| `first_gold_latency_s` | Producer start to first Gold Delta commit after the run baseline. |
| `rows_added_bronze` | Bronze rows added since the run baseline across the three intraday tables. |
| `rows_added_silver` | Silver rows added since the run baseline across the three intraday tables. |
| `rows_added_gold` | Gold rows added since the run baseline. Gold is an aggregate table, so this is not expected to equal Bronze/Silver. |
| `silver_to_bronze_ratio` | `rows_added_silver / rows_added_bronze`. For this benchmark it should be close to 1.0. |
| `bronze_expected_ok` | `true` when Bronze row delta equals `expected_rows`. |
| `silver_expected_ok` | `true` when Silver row delta equals `expected_rows`. |
| `row_integrity_ok` | `true` when Bronze and Silver both match `expected_rows` and `silver_to_bronze_ratio == 1.0`. |
| `gold_rows_current` | Current Gold row count after the run. This is cumulative unless the benchmark environment is reset. |
| `gold_refreshed` | `true` when Gold produced at least one new commit/snapshot signal for the run. |
| `bronze_throughput_rps` | Bronze row delta divided by configured burst duration. This is workload throughput, not continuous engine capacity. |
| `silver_throughput_rps` | Silver row delta divided by configured burst duration. |
| `gold_throughput_rps` | Gold row delta divided by configured burst duration. Often less useful because Gold is aggregated. |
| `bronze_ok` | `true` when Bronze produced any new rows for the run. Use `bronze_expected_ok` for stricter validation. |
| `silver_ok` | `true` when Silver produced any new rows for the run. Use `silver_expected_ok` for stricter validation. |
| `gold_ok` | `true` when Gold refreshed during the run. |
| `engine_ram_mb` | Peak Spark worker RSS sampled during the run. |
| `coord_ram_mb` | Peak Spark master/coordinator RSS sampled during the run. |

`staleness_*.csv` is a time series sampled while each run is active:

| Metric | Meaning |
|--------|---------|
| `pipeline` | Pipeline id. |
| `run_label` | Stable label combining pipeline, rate, and run number. |
| `users_per_tick` | Request-rate tier for the sample. |
| `req_per_sec` | Intended logical event rate for the sample's run. |
| `is_warmup` | Whether the sample belongs to a warmup run. |
| `wall_s` | Seconds since the run started when the sample was collected. |
| `staleness_s` | Gold data age in seconds at that sample. Lower is fresher. |

For paper/report comparisons, prioritize `pipeline_e2e_s`, `processing_overhead_s`,
`avg_staleness_s`, `max_staleness_s`, `row_integrity_ok`,
`silver_to_bronze_ratio`, and `engine_ram_mb`.

## Measurement Methodology

### Row-count polling — O(1) per poll

Delta Lake accumulates one `_delta_log/*.json` commit file per micro-batch. Naively
reading all log files to count rows would become O(n) as files accumulate across runs,
making later rate tiers artificially slower to poll.

`benchmark.py` uses a `_delta_count_cache` keyed by `(table_path, latest_commit_timestamp_ms)`.
When the table has not changed since the last poll the cached count is returned immediately,
so each poll is effectively O(1) — matching Pipeline B's Iceberg REST snapshot check.

### Gold commit tracker (replaces post-Silver wait)

`spark_gold_streaming.py` uses `readStream` on Silver Delta tables — once Silver
stops writing new commits, Gold's `foreachBatch` receives empty batches and
returns without committing. Blocking on a Gold commit **after Silver stabilises**
would hang for `STABLE_MAX_WAIT` seconds.

Instead, a background `_track_gold_commits()` thread runs from `t0` and records
the wall-clock time of every Gold Delta commit (timestamp > `base_g_ts`). It also
populates `first_gold_wall` on the first observed commit.

After Bronze and Silver both stabilise, an **adaptive settle** polls the tracker:
wait until Gold has been idle (no new commits) for `GOLD_TRIGGER_SECONDS * 2 + 15s`
(default 75 s), capped at `STABLE_MAX_WAIT` total. `t_gold` is set to the wall
time of the last observed commit.

This approach correctly handles Gold's incremental-streaming behaviour: Gold may
finish processing Silver's last micro-batch slightly before or after Silver's
final stability check, and the tracker captures it either way.

### Gold streaming job: `_refresh_lock` removed

Earlier versions of `spark_gold_streaming.py` guarded `refresh_gold()` with a
non-blocking `_refresh_lock` "to prevent concurrent refreshes." Spark Structured
Streaming with `foreachBatch` is inherently sequential — batch N+1 never starts
before batch N's call returns — so the lock had no protective value but caused
Silver micro-batches arriving during an active refresh to be **permanently dropped**
(Spark advances the checkpoint even when `foreachBatch` returns early).

In a finite-burst benchmark this produced an incomplete Gold aggregate (visible
as a much-too-low `rows_added_gold`). The lock was removed so every Silver
micro-batch is now processed. `gold_e2e_s` is correspondingly larger than before
the fix because Gold now does all the work it should.

### Deprecated: OPTIMIZE between rate tiers

The current benchmark does **not** call OPTIMIZE inside the measurement loop.
The notes below describe an older maintenance experiment and should not be
treated as part of the official latency benchmark.

Earlier versions ran Delta OPTIMIZE after each rate tier (except the last)
on all watched tables inside the Spark master container:

- **Bronze** — all 11 topics: `vitals`, `activity`, `context`, `profile`, `sleep`,
  `hrv_summary`, `breathing_summary`, `vitals_daily`,
  `heart_rate_intraday`, `hrv_intraday`, `breathing_intraday`
- **Silver** — 3 intraday tables: `heart_rate_intraday`, `hrv_intraday`, `breathing_intraday`
- **Gold** — `daily_intraday_summary`

OPTIMIZE compacts small Parquet data files written by concurrent micro-batches,
keeping read performance consistent across rate tiers without touching the
`_delta_log` JSON files (commit history is preserved). VACUUM is intentionally
**not** run because the streaming jobs' checkpoint state may still reference
recently-superseded Parquet files.

`SKIP_COMPACT` is no longer part of the official benchmark path because OPTIMIZE
is not called from the benchmark loop.
