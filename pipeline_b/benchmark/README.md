# Pipeline B Benchmark Guide

Pipeline B is benchmarked on the realtime intraday workload only:

```text
producer_realtime
-> Kafka intraday topics
-> Flink Bronze Iceberg intraday tables
-> Flink Silver Iceberg intraday tables
-> Flink Gold daily_intraday_summary
```

The benchmark excludes batch/daily/profile producers and the AI training job.
It measures the streaming path from realtime producer input to Gold aggregate
freshness.

## Default Scope

The Makefile and runner default to:

```make
PARALLELISM ?= 6
BRONZE_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
SILVER_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
GOLD_ONLY ?= daily_intraday_summary
KAFKA_PARTITIONS ?= 12
ICEBERG_MONITOR_INTERVAL ?= 15s
FLINK_CHECKPOINT_INTERVAL ?= 15 s
FLINK_MINI_BATCH_INTERVAL ?= 5 s
FLINK_MINI_BATCH_SIZE ?= 5000
REQUEST_RATES ?= 50,100,200
N_RUNS ?= 3
WARMUP_RUNS ?= 1
WARMUP_SECS ?= 10
```

`REQUEST_RATES` is mapped to `USERS_PER_TICK`; every user tick emits 3 Kafka
records: heart rate, HRV, and breathing.

## Run

From this repository:

```bash
cd ~/projects/spark-flink-compare/pipeline_b
docker compose up -d
bash benchmark/run_benchmark.sh
```

The runner checks required containers, initializes Kafka/Iceberg when needed,
and starts Bronze, Silver, and Gold Flink jobs if they are not already running.
If Silver or Gold jobs are started fresh, their Iceberg tables are reset first
to avoid schema conflicts from older batch-table layouts.

Smoke test:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
STABLE_MAX_WAIT=120 \
bash benchmark/run_benchmark.sh
```

Official local profile:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=50,100,200 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=10 \
STABLE_MAX_WAIT=300 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

## Verify Job Scope

```bash
make list-jobs
```

Expected running jobs:

```text
bronze.heart_rate_intraday
bronze.hrv_intraday
bronze.breathing_intraday
silver.heart_rate_intraday
silver.hrv_intraday
silver.breathing_intraday
gold.daily_intraday_summary
```

## Request Rate Mapping

With default `DELAY=0.1`:

| USERS_PER_TICK | Approx rows/s |
| --- | ---: |
| 50 | 1500 |
| 100 | 3000 |
| 200 | 6000 |

For lighter smoke tests:

| USERS_PER_TICK | Approx rows/s |
| --- | ---: |
| 5 | 150 |
| 10 | 300 |
| 20 | 600 |

## Output

Results are written under `benchmark/`:

```text
results_YYYYMMDD_HHMMSS.csv
staleness_YYYYMMDD_HHMMSS.csv
benchmark_YYYYMMDD_HHMMSS.log
```

Main report metrics:

| Metric | Meaning |
| --- | --- |
| `pipeline_e2e_s` | Producer start until Bronze/Silver drain and Gold has settled. |
| `gold_e2e_s` | Producer start to the last observed Gold Iceberg snapshot for the run. |
| `first_gold_latency_s` | Producer start to the first Gold Iceberg snapshot after the run baseline. |
| `processing_overhead_s` | Time after producer stop until pipeline completion. |
| `avg_staleness_s` | Average age of Gold data during the run. |
| `max_staleness_s` | Worst sampled Gold data age during the run. |
| `bronze_lag_s` | Producer stop until Bronze row deltas reach the expected count. |
| `silver_lag_s` | Producer stop until Silver row deltas reach the expected count. |
| `gold_lag_s` | Producer stop until Gold's last observed snapshot. |
| `row_integrity_ok` | True when Bronze and Silver both match expected intraday rows. |
| `silver_to_bronze_ratio` | Should be close to 1.0 for this realtime benchmark. |
| `engine_ram_mb` | Peak Flink TaskManager RSS sampled during the run. |

## CSV Reference

`results_*.csv` has one row per benchmark run. Warmup rows are retained for
auditability but excluded from printed per-rate averages.

Important columns:

| Column | Meaning |
| --- | --- |
| `timestamp` | UTC timestamp when the result row was written. |
| `pipeline` | Pipeline id: Flink streaming + Iceberg. |
| `run` | Run number inside the request-rate tier. |
| `is_warmup` | Whether this row belongs to a warmup run. |
| `users_per_tick` | Synthetic users emitted by `producer_realtime.py` per tick. |
| `req_per_sec` | Intended logical row rate: `users_per_tick * 3 / DELAY`. |
| `target_ticks` | Fixed number of producer ticks for the run. |
| `expected_rows` | Expected Bronze/Silver row delta: `users_per_tick * 3 * target_ticks`. |
| `rows_added_bronze` | Bronze rows added across the three intraday tables. |
| `rows_added_silver` | Silver rows added across the three intraday tables. |
| `rows_added_gold` | Gold rows added; this is aggregate output and need not equal Silver. |
| `gold_refreshed` | True when Gold produced a new snapshot signal during the run. |
| `bronze_throughput_rps` | Bronze row delta divided by configured burst duration. |
| `silver_throughput_rps` | Silver row delta divided by configured burst duration. |
| `gold_throughput_rps` | Gold row delta divided by configured burst duration. |
| `coord_ram_mb` | Peak Flink JobManager RSS sampled during the run. |

`staleness_*.csv` is a time series collected while each run is active:

| Column | Meaning |
| --- | --- |
| `run_label` | Stable label combining pipeline, rate, and run number. |
| `wall_s` | Seconds since the run started when the sample was collected. |
| `staleness_s` | Gold data age in seconds. Lower is fresher. |

## Measurement Notes

### Iceberg snapshot polling

Snapshot checks use the Iceberg REST catalog API. The latest snapshot metadata is
returned in a single HTTP round-trip, so the polling cost does not grow with the
number of historical snapshots.

### Gold commit tracker

The benchmark starts a background tracker at producer start and records every
Gold Iceberg snapshot newer than the run baseline. After Bronze and Silver
stabilize, the runner waits until Gold has been idle for
`GOLD_CHECKPOINT_SECONDS * 2 + 15s`, capped by `STABLE_MAX_WAIT`, then uses the
last observed Gold snapshot as the end-to-end completion point.
