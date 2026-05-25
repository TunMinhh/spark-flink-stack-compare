# Pipeline B Benchmark Plan

Pipeline B is benchmarked in intraday-only mode by default. This measures the realtime path:

```text
producer_realtime
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
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
REQUEST_RATES=5,10,15,20 \
N_RUNS=3 \
WARMUP_RUNS=1 \
WARMUP_SECS=30 \
STABLE_MAX_WAIT=300 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

Request-rate mapping with default `DELAY=0.1`:

| USERS_PER_TICK | Approx req/s |
|----------------|--------------|
| 50 | 1,500 |
| 500 | 15,000 |
| 1500 | 45,000 |

If time is limited, run one measured run per level first:

```bash
PYTHONUNBUFFERED=1 \
REQUEST_RATES=5,10,15,20 \
N_RUNS=1 \
WARMUP_RUNS=1 \
WARMUP_SECS=30 \
STABLE_MAX_WAIT=300 \
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
