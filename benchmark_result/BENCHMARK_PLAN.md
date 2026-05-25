# Benchmark Plan - Pipeline A vs Pipeline B

This plan compares the same realtime intraday workload on both pipelines:

```text
producer_realtime.py
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
```

Pipeline A uses Spark Structured Streaming + Delta Lake + HDFS. Pipeline B uses Flink + Iceberg + MinIO. Run the two stacks separately on the same VM because ports and container names overlap.

## Fairness Contract

| Parameter | Pipeline A | Pipeline B |
|---|---:|---:|
| Input path | Intraday producer only | Intraday producer only |
| Static CSV pre-seed | No | No |
| Request rates | `50`, `100`, `200` users/tick | `50`, `100`, `200` users/tick |
| Approx offered load | `1500`, `3000`, `6000` events/s | `1500`, `3000`, `6000` events/s |
| Warmup runs per rate | `1` | `1` |
| Measured runs per rate | `3` | `3` |
| Burst duration | `10s` equivalent, `100` fixed ticks | `10s` equivalent, `100` fixed ticks |
| Reset between request rates | Yes | Yes |
| Reset between warmup/measured runs inside one rate | No | No |
| Stable wait timeout | `800s` | `800s` |
| Kafka partitions | `12` | `12` |
| Main compute knob | `SHUFFLE_PARTITIONS=18` | `PARALLELISM=6` |
| Trigger/checkpoint | Spark triggers `30s` | Flink checkpoints `15s` |
| Gold target | `daily_intraday_summary` | `daily_intraday_summary` |

Each request-rate tier starts from a clean pipeline state. Inside that tier, the warmup run and the three measured runs execute continuously without resetting state. This keeps rate `100` from inheriting table history from rate `50`, while still measuring steady behavior across repeated runs at the same rate.

## Pipeline A: Spark

Run from the VM:

```bash
cd ~/data-pipeline-comparison/pipeline_a
```

Smoke test:

```bash
docker compose down -v
docker compose up -d
make init

PYTHONUNBUFFERED=1 \
REQUEST_RATES=200 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
SHUFFLE_PARTITIONS=18 \
bash benchmark/run_benchmark.sh
```

Official benchmark with reset between request rates:

```bash
for rate in 50 100 200; do
  echo "===== Spark rate=$rate ====="

  docker compose down -v
  docker compose up -d
  sleep 30
  make init

  REQUEST_RATES=$rate \
  N_RUNS=3 \
  WARMUP_RUNS=1 \
  WARMUP_SECS=10 \
  SHUFFLE_PARTITIONS=18 \
  PYTHONUNBUFFERED=1 \
  bash benchmark/run_benchmark.sh

  echo "===== Done Spark rate=$rate ====="
done
```

Expected first run baseline for each rate after reset:

```text
baseline bronze=0 silver=0 gold=0 rows
```

Spark benchmark notes:

- The runner starts missing Bronze/Silver/Gold streaming jobs automatically.
- Delta `OPTIMIZE` must not run inside the benchmark loop.
- If a run has `row_integrity_ok=false`, do not use it for headline comparison.

## Pipeline B: Flink

Run from the VM:

```bash
cd ~/data-pipeline-comparison/pipeline_b
```

Smoke test:

```bash
make cancel-all || true
make reset
make init

PYTHONUNBUFFERED=1 \
REQUEST_RATES=200 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
PARALLELISM=6 \
bash benchmark/run_benchmark.sh
```

Official benchmark with reset between request rates:

```bash
for rate in 50 100 200; do
  echo "===== Flink rate=$rate ====="

  make cancel-all || true
  make reset
  make init

  REQUEST_RATES=$rate \
  N_RUNS=3 \
  WARMUP_RUNS=1 \
  WARMUP_SECS=10 \
  PARALLELISM=6 \
  PYTHONUNBUFFERED=1 \
  bash benchmark/run_benchmark.sh

  echo "===== Done Flink rate=$rate ====="
done
```

Expected first run baseline for each rate after reset:

```text
baseline bronze=0 silver=0 gold=0 rows
```

Flink benchmark notes:

- The runner starts missing Bronze/Silver/Gold Flink jobs automatically.
- It does not pre-seed static CSV data for the official workload.
- If a run has `row_integrity_ok=false`, do not use it for headline comparison.

## Running Safely Over SSH

For long runs, use `tmux` if it is installed:

```bash
tmux new -s spark_bench
```

Detach without stopping the benchmark:

```text
Ctrl-b, then d
```

Reattach:

```bash
tmux attach -t spark_bench
```

If `tmux` is not installed:

```bash
sudo apt update
sudo apt install -y tmux
```

## Results

Each rate produces its own timestamped output set:

```bash
ls -lt benchmark/results_*.csv benchmark/staleness_*.csv benchmark/benchmark_*.log | head
```

Keep these files for each rate and pipeline:

```text
results_YYYYMMDD_HHMMSS.csv
staleness_YYYYMMDD_HHMMSS.csv
benchmark_YYYYMMDD_HHMMSS.log
```

Primary metrics:

```text
req_per_sec
gold_e2e_s / pipeline_e2e_s
first_gold_latency_s
avg_staleness_s
max_staleness_s
bronze_lag_s
silver_lag_s
gold_lag_s
producer_actual_rps
catchup_ratio
silver_to_bronze_ratio
row_integrity_ok
engine_ram_mb
```

Interpretation rules:

- Exclude `is_warmup=true` from reported averages.
- Use only runs with `row_integrity_ok=true` for headline latency comparison.
- If Spark times out but later catches up, record it as a non-converged run for that timeout window, not data loss.
- Because both pipelines use different table formats and commit protocols, report results as end-to-end workload measurements, not identical operator-level measurements.
