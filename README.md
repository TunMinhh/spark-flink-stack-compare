# Data Pipeline Comparison Benchmark

This repository compares two realtime wearable streaming pipelines:

- Pipeline A: Spark Structured Streaming + Delta Lake + HDFS
- Pipeline B: Apache Flink + Apache Iceberg + MinIO/S3

The benchmark measures the same realtime intraday workload in both pipelines:

```text
producer_realtime.py
-> Kafka realtime topics
-> Bronze intraday tables
-> Silver intraday tables
-> Gold daily_intraday_summary
```

The producer emits three signal families per tick: heart rate, HRV, and
breathing. With `DELAY=0.1`, `REQUEST_RATES=200` means roughly
`200 * 3 / 0.1 = 6000` offered rows per second.

The benchmark does not run AI training, dashboards, historical backfill, or any
daily/hourly topic pre-seeding. The hourly/daily CSV files are only used by
`producer_realtime.py` as baseline values for generating intraday records.

## Data Files

Put the shared CSV files here:

```text
data/hourly_fitbit_sema_df_unprocessed.csv
data/daily_fitbit_sema_df_unprocessed.csv
```

Both pipelines read those files through `../data/...` paths. Do not copy them
into `pipeline_a/` or `pipeline_b/`.

## Default Benchmark Profile

| Setting | Pipeline A | Pipeline B |
| --- | --- | --- |
| Request rates | `50,100,200` users/tick | `50,100,200` users/tick |
| Approx offered load | `1500,3000,6000` rows/s | `1500,3000,6000` rows/s |
| Runs per rate | `3` | `3` |
| Warmup runs per rate | `1` | `1` |
| Producer burst duration | `10s` | `10s` |
| Kafka partitions | `12` | `12` |
| Main tuning knob | `SHUFFLE_PARTITIONS=18` | `PARALLELISM=6` |
| Engine memory budget | about `18g` Spark executor+overhead | `18432m` Flink TaskManager process |
| Engine container cap | `24g` | `24g` |

Run only one pipeline at a time for a fair single-VM comparison.

## Pipeline A

```bash
cd pipeline_a
cp .env.example .env
docker compose up -d --build
bash benchmark/run_benchmark.sh
```

Smoke test:

```bash
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

## Pipeline B

```bash
cd pipeline_b
cp .env.example .env
docker compose up -d --build
bash benchmark/run_benchmark.sh
```

Smoke test:

```bash
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
STABLE_MAX_WAIT=120 bash benchmark/run_benchmark.sh
```

## Outputs

Each run writes:

```text
benchmark/results_YYYYMMDD_HHMMSS.csv
benchmark/staleness_YYYYMMDD_HHMMSS.csv
benchmark/benchmark_YYYYMMDD_HHMMSS.log
```

Use these metrics for comparison:

```text
pipeline_e2e_s
processing_overhead_s
first_gold_latency_s
avg_staleness_s
max_staleness_s
row_integrity_ok
silver_to_bronze_ratio
engine_ram_mb
```

Interpret results as end-to-end workload measurements for this repository's
realtime wearable workload, not as universal Spark-vs-Flink claims.
