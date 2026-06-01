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
| Trigger/checkpoint | Spark triggers `15s` | Flink checkpoints `15s` |
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

## Metric Dictionary

The benchmark result CSV files use one row per run. `is_warmup=true` rows are
kept for traceability but should be excluded from final averages. The formulas
below mirror the benchmark runner code in `pipeline_a/benchmark/benchmark.py`
and `pipeline_b/benchmark/benchmark.py`; they are runner-defined measurement
rules, not guessed from metric names.

### Workload and run identity

| Metric | Meaning | How it is calculated |
|---|---|---|
| `timestamp` | UTC time when the run result was written. | `datetime.now(timezone.utc).isoformat()` at result creation. |
| `pipeline` | Pipeline label. | `A` for Spark, `B` for Flink. |
| `run` | Run number within the request-rate tier. | Warmup and measured runs are numbered by the benchmark runner. |
| `is_warmup` | Whether the row is a warmup run. | `true` for warmup runs, `false` for measured runs. |
| `users_per_tick` | Producer users emitted per tick. | From `REQUEST_RATES`; e.g. `50`, `100`, `200`. |
| `req_per_sec` | Offered event rate in rows/s. | `users_per_tick * 3 / REALTIME_DELAY`. The `3` is heart-rate, HRV, and breathing topics. |
| `warmup_secs` | Burst duration used for throughput normalization. | `WARMUP_SECS`, normally `10`. |
| `target_ticks` | Fixed number of producer ticks per run. | `MAX_TICKS`, normally `100`. |
| `expected_rows` | Expected rows added to Bronze and Silver. | `users_per_tick * 3 * target_ticks`. |

### Latency and freshness

| Metric | Meaning | How it is calculated |
|---|---|---|
| `gold_e2e_s` | End-to-end time until the pipeline is considered complete for the run. | Same value as `pipeline_e2e_s`; `max(bronze_done_s, silver_done_s, gold_done_s)` from producer start. |
| `pipeline_e2e_s` | End-to-end completion time for Bronze, Silver, and Gold. | `max(t_bronze, t_silver, t_gold)`, measured from producer start. |
| `producer_stop_s` | Time when the producer finished sending the burst. | Wall-clock seconds from producer start to producer process completion. |
| `processing_overhead_s` | Catch-up time after input finished. | `max(0, pipeline_e2e_s - producer_stop_s)`. |
| `bronze_lag_s` | Bronze catch-up lag after producer stop. | `max(0, t_bronze - producer_stop_s)`. |
| `silver_lag_s` | Silver catch-up lag after producer stop. | `max(0, t_silver - producer_stop_s)`. |
| `gold_lag_s` | Gold catch-up lag after producer stop. | `max(0, t_gold - producer_stop_s)`. |
| `first_gold_latency_s` | Time to the first observed Gold commit/refresh. | First Gold commit wall-clock time from producer start; `-1` if not observed. |
| `avg_staleness_s` | Average age of the latest Gold output while monitoring. | Average of `now - latest_gold_commit_time` samples. |
| `max_staleness_s` | Worst observed Gold freshness gap. | Maximum sampled staleness in seconds. |
| `min_staleness_s` | Best observed Gold freshness gap. | Minimum sampled staleness in seconds. |
| `catchup_ratio` | How many burst durations the pipeline needed to catch up after input ended. | `processing_overhead_s / warmup_secs`. Lower is better. |

### Rows and throughput

| Metric | Meaning | How it is calculated |
|---|---|---|
| `producer_actual_rps` | Actual accepted input rate measured from Bronze rows. | `rows_added_bronze / producer_stop_s`. |
| `rows_added_bronze` | New Bronze rows written during the run. | Bronze row count after run minus Bronze baseline before run. |
| `rows_added_silver` | New Silver rows written during the run. | Silver row count after run minus Silver baseline before run. |
| `rows_added_gold` | New Gold aggregate rows created during the run. | Gold row count after run minus Gold baseline before run. For upsert aggregates this can be `0` even when Gold refreshed. |
| `silver_to_bronze_ratio` | Silver retention compared with Bronze. | `rows_added_silver / rows_added_bronze`. Expected value is `1.0`. |
| `bronze_throughput_rps` | Bronze throughput normalized by burst duration. | `rows_added_bronze / warmup_secs`. |
| `silver_throughput_rps` | Silver throughput normalized by burst duration. | `rows_added_silver / warmup_secs`. |
| `gold_throughput_rps` | Gold row throughput. | Currently `-1` because Gold is an upsert aggregate table, so row count is not a valid throughput signal. |
| `gold_rows_current` | Current total rows in Gold after the run. | `gold_baseline_rows + rows_added_gold`. |

### Correctness and convergence flags

| Metric | Meaning | How it is calculated |
|---|---|---|
| `bronze_expected_ok` | Bronze received exactly the expected run rows. | `rows_added_bronze == expected_rows`. |
| `silver_expected_ok` | Silver received exactly the expected run rows. | `rows_added_silver == expected_rows`. |
| `row_integrity_ok` | Main row-count integrity gate. | `bronze_expected_ok and silver_expected_ok and silver_to_bronze_ratio == 1.0`. |
| `bronze_ok` | Bronze converged within the timeout and passed row-count check. | Stability wait succeeded and `bronze_expected_ok=true`. |
| `silver_ok` | Silver converged within the timeout and passed row-count check. | Stability wait succeeded and `silver_expected_ok=true`. |
| `gold_ok` | Whether Gold produced at least one observed commit/refresh during the run. | `true` if the Gold commit tracker recorded a commit; `false` if no Gold commit was observed. |
| `gold_refreshed` | Whether a Gold refresh was observed. | Same value as `gold_ok` in the result rows. |

### Resource metrics

| Metric | Meaning | How it is calculated |
|---|---|---|
| `engine_ram_mb` | Peak RAM of the main processing engine container. | Spark: `spark-worker`; Flink: `flink-taskmanager`. Sampled from Docker stats during the run. |
| `coord_ram_mb` | Peak RAM of the coordinator/master container. | Spark: `spark-master`; Flink: `flink-jobmanager`. Sampled from Docker stats during the run. |

### Staleness CSV

The `staleness_*.csv` files store the raw samples used to compute
`avg_staleness_s`, `max_staleness_s`, and `min_staleness_s`.

| Metric | Meaning | How it is calculated |
|---|---|---|
| `pipeline` | Pipeline label for the sample. | `A` for Spark, `B` for Flink. |
| `run_label` | Human-readable run identifier. | Includes offered rate and run number. |
| `users_per_tick` | Producer users per tick for this run. | Same as result CSV. |
| `req_per_sec` | Offered event rate. | Same as result CSV. |
| `is_warmup` | Whether the sample belongs to a warmup run. | Same as result CSV. |
| `wall_s` | Sample time. | Seconds from producer start when the staleness sample was recorded. |
| `staleness_s` | Gold output age at that sample. | `sample_time - latest_observed_gold_commit_time`. Lower means fresher Gold output. |

Interpretation rules:

- Exclude `is_warmup=true` from reported averages.
- Use only runs with `row_integrity_ok=true` for headline latency comparison.
- If Spark times out but later catches up, record it as a non-converged run for that timeout window, not data loss.
- Because both pipelines use different table formats and commit protocols, report results as end-to-end workload measurements, not identical operator-level measurements.
