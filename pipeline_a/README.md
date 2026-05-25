# Pipeline A Benchmark Plan

Pipeline A now uses the same default benchmark workload as Pipeline B: the realtime intraday path only.
Bronze, Silver, and Gold run as Spark Structured Streaming jobs.

```text
producer_realtime
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
```

## Default Settings

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

`SHUFFLE_PARTITIONS=18` matches the 32-vCPU / 64-GB VM benchmark profile; Pipeline B uses `PARALLELISM=6`.

## 1. Start Pipeline A

```bash
cd ~/data-pipeline-comparison/pipeline_a
git pull

docker compose up -d
make init
```

By default, Compose starts the realtime ingest/processing stack plus MLflow,
FastAPI, Postgres sink, and Grafana.

`benchmark/run_benchmark.sh` starts the three streaming jobs automatically if
they are not already running:

```text
make bronze  -> spark_bronze.py
make silver  -> spark_silver_streaming.py
make gold    -> spark_gold_streaming.py
```

Hourly/daily producers and batch rebuild jobs are not part of the default
pipeline path. Use `make train-ai` after Silver has realtime data to train
AttentionLSTM on the intraday sequence tables, then `make export-gold` to
publish `daily_intraday_summary` and `ai_intraday_insights` to Postgres.

## 2. Smoke Test

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
SHUFFLE_PARTITIONS=18 \
bash benchmark/run_benchmark.sh
```

Pass criteria:

```text
Silver rows added > 0
Gold rows added > 0
benchmark/results_*.csv is created
```

## 3. Official Benchmark

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5,10,15,20 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=30 \
SHUFFLE_PARTITIONS=18 \
bash benchmark/run_benchmark.sh
```

Request-rate mapping with default `DELAY=0.1`:

| USERS_PER_TICK | Approx req/s |
|----------------|--------------|
| 50 | 1,500 |
| 500 | 15,000 |
| 1500 | 45,000 |

## 4. Result Files

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
silver_throughput_rps
gold_throughput_rps
engine_ram_mb
```

More details are in [benchmark/README.md](benchmark/README.md).
