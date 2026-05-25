# Pipeline B Benchmark Guide

Pipeline B benchmarks the realtime intraday streaming path by default:

```text
producer_realtime
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
```

This keeps the benchmark focused on the realtime workload and avoids unrelated daily/profile/wellness aggregations affecting checkpoint time.

## Default Scope

The Makefile defaults are:

```make
PARALLELISM ?= 6
BRONZE_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
SILVER_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
GOLD_ONLY ?= daily_intraday_summary
```

To run the full medallion jobs instead, explicitly clear the scope variables:

```bash
BRONZE_ONLY= SILVER_ONLY= GOLD_ONLY= make bronze
BRONZE_ONLY= SILVER_ONLY= GOLD_ONLY= make silver
BRONZE_ONLY= SILVER_ONLY= GOLD_ONLY= make gold
```

## Run Intraday Benchmark

Start or restart the intraday pipeline:

```bash
cd ~/data-pipeline-comparison/pipeline_b

make cancel-all

PARALLELISM=6 make bronze
sleep 20

PARALLELISM=6 make silver
sleep 30

PARALLELISM=6 make gold
```

Check that the Flink jobs are scoped to intraday:

```bash
make list-jobs
```

Expected job names should include only:

```text
bronze.heart_rate_intraday, bronze.hrv_intraday, bronze.breathing_intraday
silver.heart_rate_intraday, silver.hrv_intraday, silver.breathing_intraday
gold.daily_intraday_summary
```

Run the benchmark:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5,10,15,20 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=30 \
STABLE_MAX_WAIT=300 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

For a quick smoke test:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
STABLE_MAX_WAIT=120 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

## Output

Results are written to:

```text
benchmark/results_YYYYMMDD_HHMMSS.csv
benchmark/staleness_YYYYMMDD_HHMMSS.csv
benchmark/benchmark_YYYYMMDD_HHMMSS.log
```

## Metrics

| Metric | Meaning |
|--------|---------|
| `gold_e2e_s` | Producer start → wall-clock time of Gold Iceberg's **last** snapshot during the run. Derived from a real-time commit tracker. |
| `first_gold_latency_s` | Producer start → wall-clock time of the **first** Gold Iceberg snapshot after `base_g_ts` (Gold's state captured before the run). Captured by the same tracker thread. |
| `avg_staleness_s` | Average Gold snapshot staleness sampled every `STALENESS_POLL_SECS` seconds during the run |
| `max_staleness_s` | Maximum Gold snapshot staleness during the burst |
| `gold_lag_s` | Time between producer stop and Gold's last snapshot |
| `gold_throughput_rps` | Gold rows committed per second during the burst |
| `engine_ram_mb` | Peak Flink TaskManager RSS sampled during the run |

### Full CSV Reference

`results_*.csv` has one row per benchmark run. Warmup rows are kept in the CSV
for auditability, but rate summaries exclude rows where `is_warmup=true`.

| Metric | Meaning |
|--------|---------|
| `timestamp` | UTC timestamp when the run result row was written. |
| `pipeline` | Pipeline id. Pipeline B is Flink streaming + Iceberg. |
| `run` | Run number inside the current request-rate tier. Warmup runs start at 1. |
| `is_warmup` | `true` for warmup runs. These are not included in printed per-rate averages. |
| `users_per_tick` | Number of synthetic users emitted by `producer_realtime.py` per producer tick. |
| `req_per_sec` | Intended logical event rate: `users_per_tick * 3 / DELAY`, because each user tick emits 3 intraday records. |
| `warmup_secs` | Historical name for producer burst duration. With fixed ticks, it is the target burst duration equivalent. |
| `target_ticks` | Fixed number of producer ticks for the run. Default is 100. |
| `expected_rows` | Expected Bronze/Silver row delta for the run: `users_per_tick * 3 * target_ticks`. |
| `avg_staleness_s` | Average Gold staleness sampled during the run. Staleness is `now - latest Gold Iceberg snapshot timestamp`. |
| `max_staleness_s` | Worst sampled Gold staleness during the run. Higher values mean Gold was serving older data. |
| `min_staleness_s` | Best sampled Gold staleness during the run. |
| `gold_e2e_s` | Producer start to full pipeline completion for the measured run. In the current method this equals `pipeline_e2e_s`. |
| `pipeline_e2e_s` | Producer start to the point where Bronze/Silver have drained and Gold has settled. This is the main end-to-end latency metric. |
| `producer_stop_s` | Seconds from producer start until the producer finishes emitting the fixed workload. |
| `producer_actual_rps` | Actual rows emitted per second, computed from `expected_rows / producer_stop_s`. This can be lower than `req_per_sec` if producer startup or IO adds overhead. |
| `processing_overhead_s` | Time after producer stop until the pipeline is considered complete: `pipeline_e2e_s - producer_stop_s`. |
| `catchup_ratio` | `processing_overhead_s / warmup_secs`. Values greater than 1 mean catch-up took longer than the input burst duration. |
| `bronze_lag_s` | Seconds after producer stop until watched Bronze Iceberg tables reach expected rows and stop changing. |
| `silver_lag_s` | Seconds after producer stop until watched Silver Iceberg tables reach expected rows and stop changing. |
| `gold_lag_s` | Seconds after producer stop until the last observed Gold Iceberg snapshot used for this run. |
| `first_gold_latency_s` | Producer start to first Gold Iceberg snapshot after the run baseline. |
| `rows_added_bronze` | Bronze rows added since the run baseline across the three intraday tables. |
| `rows_added_silver` | Silver rows added since the run baseline across the three intraday tables. |
| `rows_added_gold` | Gold rows added since the run baseline. Gold is an aggregate table, so this is not expected to equal Bronze/Silver. |
| `silver_to_bronze_ratio` | `rows_added_silver / rows_added_bronze`. For this benchmark it should be close to 1.0. |
| `bronze_expected_ok` | `true` when Bronze row delta equals `expected_rows`. |
| `silver_expected_ok` | `true` when Silver row delta equals `expected_rows`. |
| `row_integrity_ok` | `true` when Bronze and Silver both match `expected_rows` and `silver_to_bronze_ratio == 1.0`. |
| `gold_rows_current` | Current Gold row count after the run. This is cumulative unless the benchmark environment is reset. |
| `gold_refreshed` | `true` when Gold produced at least one new snapshot signal for the run. |
| `bronze_throughput_rps` | Bronze row delta divided by configured burst duration. This is workload throughput, not continuous engine capacity. |
| `silver_throughput_rps` | Silver row delta divided by configured burst duration. |
| `gold_throughput_rps` | Gold row delta divided by configured burst duration. Often less useful because Gold is aggregated. |
| `bronze_ok` | `true` when Bronze produced any new rows for the run. Use `bronze_expected_ok` for stricter validation. |
| `silver_ok` | `true` when Silver produced any new rows for the run. Use `silver_expected_ok` for stricter validation. |
| `gold_ok` | `true` when Gold refreshed during the run. |
| `engine_ram_mb` | Peak Flink TaskManager RSS sampled during the run. |
| `coord_ram_mb` | Peak Flink JobManager RSS sampled during the run. |

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

### Gold commit tracker

A background `_track_gold_commits()` thread runs from `t0` and records the
wall-clock time of every Gold Iceberg snapshot (timestamp > `base_g_ts`).
It also populates `first_gold_wall` on the first observed snapshot.

After Bronze and Silver both stabilise, an **adaptive settle** polls the tracker:
wait until Gold has been idle (no new snapshots) for
`GOLD_CHECKPOINT_SECONDS * 2 + 15 s` (default 45 s, matching Flink's 15 s
checkpoint interval), capped at `STABLE_MAX_WAIT` total. `t_gold` is set to
the wall-clock time of the last observed snapshot.

This methodology mirrors Pipeline A's benchmark exactly so `gold_e2e_s` is
directly comparable between the two pipelines. Previously the benchmark waited
for a Gold snapshot newer than the one captured right after Silver stabilised
(`post_silver_g_ts`); that approach was fragile because Flink's Iceberg sink
only commits snapshots when there is in-flight data to write — once the pipeline
fully drains, no further snapshot is produced and the wait would hang for
`STABLE_MAX_WAIT` seconds.

### Row-count polling — O(1) per poll

Snapshot existence checks use the Iceberg REST catalog API
(`GET /v1/namespaces/{ns}/tables/{table}`), which always returns the latest snapshot
metadata in a single HTTP round-trip regardless of table history size. This is
structurally O(1) and does not accumulate cost across runs.

## Request Rate Mapping

The benchmark estimates request rate as:

```text
users_per_tick * 3 / DELAY
```

With the default `DELAY=0.1`:

| USERS_PER_TICK | Approx req/s |
|----------------|--------------|
| 5 | 150 |
| 10 | 300 |
| 15 | 450 |
| 20 | 600 |
