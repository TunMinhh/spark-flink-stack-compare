# Benchmark Plan - Pipeline A vs Pipeline B

This plan compares the same realtime intraday workload on both pipelines:

```text
producer_realtime.py
-> Kafka intraday topics
-> Bronze heart_rate_intraday / hrv_intraday / breathing_intraday
-> Silver heart_rate_intraday / hrv_intraday / breathing_intraday
-> Gold daily_intraday_summary
```

Pipeline A uses Spark Structured Streaming + Delta Lake + HDFS. Pipeline B uses Flink + Iceberg + HDFS. Run the two stacks separately on the same VM because ports, container names, and RAM allocations overlap.

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
| Storage backend | HDFS | HDFS |
| Warehouse path | `hdfs://namenode:9000/data/...` | `hdfs://namenode:9000/warehouse/iceberg` |
| Main compute knob | `SHUFFLE_PARTITIONS=18` | `PARALLELISM=6` |
| Trigger/checkpoint | Spark triggers `15s` | Flink checkpoints `5s` |
| Gold target | `daily_intraday_summary` | `daily_intraday_summary` |

Each request-rate tier starts from a clean pipeline state. Inside that tier, the warmup run and the three measured runs execute continuously without resetting state. This keeps rate `100` from inheriting table history from rate `50`, while still measuring steady behavior across repeated runs at the same rate.

## Pipeline A: Spark

Run from the VM:

```bash
cd ~/spark-flink-stack-compare/pipeline_a
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
cd ~/spark-flink-stack-compare/pipeline_b
```

Smoke test:

```bash
make cancel-all || true
docker compose down -v
docker compose up -d
sleep 60
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

  docker compose down -v
  docker compose up -d
  sleep 60
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
- Iceberg uses HDFS as the warehouse. If an error still references
  `s3://iceberg`, the old Iceberg catalog volume was not removed; rerun
  `docker compose down -v`.
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
| `gold_e2e_s` | Gold-ready service latency for the run. This is the headline E2E metric used in the report. | `producer_stop_s + gold_lag_s`, measured from producer start until the final observed Gold commit for that run. |
| `pipeline_e2e_s` | End-to-end completion time for Bronze, Silver, and Gold. In the current result set it is close to `gold_e2e_s`, but it remains a separate full-pipeline settlement metric. | `max(t_bronze, t_silver, t_gold)`, measured from producer start. |
| `producer_stop_s` | Time when the producer finished sending the burst. | Wall-clock seconds from producer start to producer process completion. |
| `processing_overhead_s` | Catch-up time after input finished. | `max(0, pipeline_e2e_s - producer_stop_s)`. |
| `bronze_lag_s` | Bronze catch-up lag after producer stop. | `max(0, t_bronze - producer_stop_s)`. |
| `silver_lag_s` | Silver catch-up lag after producer stop. | `max(0, t_silver - producer_stop_s)`. |
| `gold_lag_s` | Gold catch-up lag after producer stop. | `max(0, t_gold - producer_stop_s)`. |
| `first_gold_latency_s` | Time to the first observed Gold commit/refresh. | First Gold commit wall-clock time from producer start; `-1` if not observed. |
| `avg_staleness_s` | Average age of the latest Gold output, sampled by the benchmark runner. For paper figures, prefer the post-first-Gold values in `staleness_corrected.csv` when available. | Average of sampled `now - latest_gold_commit_time` values. |
| `max_staleness_s` | Worst observed Gold staleness in the runner samples. | Maximum sampled staleness in seconds. |
| `min_staleness_s` | Best observed Gold staleness. | Minimum sampled staleness in seconds. |
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

The `staleness_*.csv` files store the raw per-sample data; every sample from
t=0 is included (pre-Gold-commit samples and post-Gold-commit samples alike).

| Column | Meaning |
|---|---|
| `pipeline` | `A` (Spark) or `B` (Flink). |
| `run_label` | Human-readable run identifier including offered rate and run number. |
| `users_per_tick` | Producer users per tick (50 / 100 / 200). |
| `req_per_sec` | Offered event rate in rows/s. |
| `is_warmup` | `True` for warmup runs; exclude from reported averages. |
| `wall_s` | Seconds from producer start when this sample was recorded. |
| `staleness_s` | `now - latest_observed_gold_commit_time` at sample time. Lower = fresher. |

### Corrected Staleness

`staleness_corrected.csv` is produced by `recompute_staleness.py`. It joins
the raw staleness samples with `first_gold_latency_s` from `results_*.csv` and
keeps only samples where `wall_s >= first_gold_latency_s` for each run. This
removes Phase 1 (pre-first-Gold-commit) samples whose staleness reflects the
inter-run idle gap rather than pipeline-induced freshness delay.

| Column | Meaning |
|---|---|
| `rate` | `users_per_tick` (50 / 100 / 200). |
| `pipeline` | `spark` or `flink`. |
| `run_label` | Same as staleness CSV. |
| `is_warmup` | Same as staleness CSV. |
| `first_gold_latency_s` | Wall time of first Gold commit; used as the trim boundary. |
| `n_samples_total` | Total samples in the raw staleness CSV for this run. |
| `n_samples_post_first_gold` | Samples at or after first Gold commit. |
| `corr_avg_s` | Corrected average staleness (post-first-Gold only). |
| `corr_max_s` | Corrected max staleness. |
| `corr_min_s` | Corrected min staleness. |
| `orig_avg_s` | Original (raw) average staleness for comparison. |
| `orig_max_s` | Original max staleness for comparison. |

To regenerate:

```bash
cd benchmark_result
python3 recompute_staleness.py
```

### Interpretation Rules

- Exclude `is_warmup=true` rows from all reported averages.
- Use only runs with `row_integrity_ok=true` for headline latency comparisons.
- Use `corr_avg_s` / `corr_max_s` from `staleness_corrected.csv` for
  staleness charts that explicitly trim pre-first-Gold samples.
- `bronze_lag_s` and `silver_lag_s` are layer catch-up measurements after the
  producer stops. For Spark/Delta, they can include the cost of committing a
  micro-batch to Delta; for Flink/Iceberg, they follow checkpoint/snapshot
  completion.
