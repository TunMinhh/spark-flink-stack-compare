# Pipeline B Benchmark Plan

Pipeline B is benchmarked in intraday-only mode by default. It now uses the
same single-node HDFS service profile as Pipeline A for the Iceberg warehouse
and Flink checkpoints:

```text
producer_realtime
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
-> Iceberg warehouse on hdfs://namenode:9000/warehouse/iceberg
```

The Makefile defaults are already set for this path:

```make
PARALLELISM ?= 6
BRONZE_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
SILVER_ONLY ?= heart_rate_intraday,hrv_intraday,breathing_intraday
GOLD_ONLY ?= daily_intraday_summary
```

## 1. Start Intraday Pipeline

```bash
cd ~/data-pipeline-comparison/pipeline_b
git pull

docker compose up -d
make init
make cancel-all

PARALLELISM=6 make bronze
sleep 20

PARALLELISM=6 make silver
sleep 30

PARALLELISM=6 make gold
```

By default, Compose starts the realtime ingest/processing stack plus MLflow,
FastAPI, Postgres sink, and Grafana.

MinIO remains in the Compose stack only for MLflow artifacts; it is no longer
the Iceberg warehouse used by the benchmark path.

The realtime producer reads shared baseline CSVs from:

```text
../data/hourly_fitbit_sema_df_unprocessed.csv
../data/daily_fitbit_sema_df_unprocessed.csv
```

Verify the Flink jobs:

```bash
make list-jobs
```

Expected job scope:

```text
Bronze: heart_rate_intraday, hrv_intraday, breathing_intraday
Silver: heart_rate_intraday, hrv_intraday, breathing_intraday
Gold: daily_intraday_summary
```

## 2. Smoke Test

Run a quick test before the official benchmark:

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

Pass criteria:

```text
Bronze rows added > 0
Silver rows added > 0
Gold rows added > 0
benchmark/results_*.csv is created
```

## 3. Official Benchmark

Run three request-rate levels, with one warmup run and three measured runs per level:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=50,100,200 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=10 \
STABLE_MAX_WAIT=800 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

Request-rate mapping with default `DELAY=0.1`:

| USERS_PER_TICK | Approx events/s |
|----------------|--------------|
| 50 | 1,500 |
| 100 | 3,000 |
| 200 | 6,000 |

If time is limited, run one measured run per level first:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=50,100,200 \
N_RUNS=1 \
WARMUP_RUNS=1 \
WARMUP_SECS=10 \
STABLE_MAX_WAIT=800 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

## 4. Result Files

After a run, find the newest outputs:

```bash
ls -lt benchmark/results_*.csv benchmark/staleness_*.csv benchmark/benchmark_*.log | head
```

Primary result file:

```text
benchmark/results_YYYYMMDD_HHMMSS.csv
```

Useful report metrics:

```text
req_per_sec
gold_e2e_s
first_gold_latency_s
avg_staleness_s
max_staleness_s
bronze_lag_s
silver_lag_s
gold_lag_s
bronze_tps
silver_tps
gold_tps
engine_ram_mb
```

More details are in [benchmark/README.md](benchmark/README.md).
