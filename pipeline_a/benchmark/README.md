# Pipeline A Benchmark Guide

Pipeline A is benchmarked on the realtime intraday workload only:

```text
producer_realtime
-> Kafka intraday topics
-> Spark Bronze Delta intraday tables
-> Spark Silver Delta intraday tables
-> Spark Gold daily_intraday_summary
```

The benchmark intentionally excludes batch/daily/profile producers and the AI
training job. It measures the streaming path from realtime producer input to
Gold aggregate freshness.

## Default Scope

The Makefile and runner default to:

```make
BRONZE_TOPICS ?= wearable_heart_rate_intraday,wearable_hrv_intraday,wearable_breathing_intraday
SILVER_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
GOLD_ONLY ?= daily_intraday_summary
KAFKA_PARTITIONS ?= 12
SHUFFLE_PARTITIONS ?= 18
BRONZE_TRIGGER_SECONDS ?= 30
SILVER_TRIGGER_SECONDS ?= 30
GOLD_TRIGGER_SECONDS ?= 30
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
cd ~/projects/spark-flink-compare/pipeline_a
docker compose up -d
bash benchmark/run_benchmark.sh
```

The runner checks required containers, runs `make init`, and starts Bronze,
Silver, and Gold streaming jobs in the background if they are not already
running.

Smoke test:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

Official local profile:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=50,100,200 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
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
| `gold_e2e_s` | Producer start to the last observed Gold Delta commit for the run. |
| `first_gold_latency_s` | Producer start to the first Gold Delta commit after the run baseline. |
| `processing_overhead_s` | Time after producer stop until pipeline completion. |
| `avg_staleness_s` | Average age of Gold data during the run. |
| `max_staleness_s` | Worst sampled Gold data age during the run. |
| `bronze_lag_s` | Producer stop until Bronze row deltas reach the expected count. |
| `silver_lag_s` | Producer stop until Silver row deltas reach the expected count. |
| `gold_lag_s` | Producer stop until Gold's last observed commit. |
| `row_integrity_ok` | True when Bronze and Silver both match expected intraday rows. |
| `silver_to_bronze_ratio` | Should be close to 1.0 for this realtime benchmark. |
| `engine_ram_mb` | Peak Spark worker RSS sampled during the run. |

## CSV Reference

`results_*.csv` has one row per benchmark run. Warmup rows are retained for
auditability but excluded from printed per-rate averages.

Important columns:

| Column | Meaning |
| --- | --- |
| `timestamp` | UTC timestamp when the result row was written. |
| `pipeline` | Pipeline id: Spark Structured Streaming + Delta Lake. |
| `run` | Run number inside the request-rate tier. |
| `is_warmup` | Whether this row belongs to a warmup run. |
| `users_per_tick` | Synthetic users emitted by `producer_realtime.py` per tick. |
| `req_per_sec` | Intended logical row rate: `users_per_tick * 3 / DELAY`. |
| `target_ticks` | Fixed number of producer ticks for the run. |
| `expected_rows` | Expected Bronze/Silver row delta: `users_per_tick * 3 * target_ticks`. |
| `rows_added_bronze` | Bronze rows added across the three intraday tables. |
| `rows_added_silver` | Silver rows added across the three intraday tables. |
| `rows_added_gold` | Gold rows added; this is aggregate output and need not equal Silver. |
| `gold_refreshed` | True when Gold produced a new commit signal during the run. |
| `bronze_throughput_rps` | Bronze row delta divided by configured burst duration. |
| `silver_throughput_rps` | Silver row delta divided by configured burst duration. |
| `gold_throughput_rps` | Gold row delta divided by configured burst duration. |
| `coord_ram_mb` | Peak Spark master/coordinator RSS sampled during the run. |

`staleness_*.csv` is a time series collected while each run is active:

| Column | Meaning |
| --- | --- |
| `run_label` | Stable label combining pipeline, rate, and run number. |
| `wall_s` | Seconds since the run started when the sample was collected. |
| `staleness_s` | Gold data age in seconds. Lower is fresher. |

## Measurement Notes

### Delta row-count polling

Delta Lake creates one `_delta_log/*.json` commit file per micro-batch. The
benchmark caches row counts by `(table_path, latest_commit_timestamp_ms)`, so a
poll returns immediately when a table has not changed. This keeps repeated polls
effectively O(1) during long benchmark sessions.

### Gold commit tracker

Gold is a streaming aggregate over Silver Delta. Once Silver has no new commits,
Gold may not produce another empty commit. The benchmark therefore starts a
background tracker at producer start and records every Gold commit newer than
the run baseline.

After Bronze and Silver stabilize, the runner waits until Gold has been idle for
`GOLD_TRIGGER_SECONDS * 2 + 15s`, capped by `STABLE_MAX_WAIT`, then uses the
last observed Gold commit as the end-to-end completion point.

### No OPTIMIZE in the loop

Delta OPTIMIZE is not part of the official benchmark loop. It is a maintenance
operation and would distort latency numbers if mixed into measured runs.
