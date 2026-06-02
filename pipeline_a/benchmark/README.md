# Pipeline A Benchmark Guide

Pipeline A benchmarks the realtime intraday Spark/Delta/HDFS path:

```text
producer_realtime
-> Kafka intraday topics
-> Spark Bronze Delta intraday tables
-> Spark Silver Delta intraday tables
-> Spark Gold daily_intraday_summary
-> Delta tables on hdfs://namenode:9000/data
```

The benchmark runs only the realtime producer and does not run AI training.
The hourly/daily CSV files are used only as baseline inputs for
`producer_realtime.py`; they are not emitted as hourly or daily Kafka topics.

## Run

```bash
cd pipeline_a
docker compose up -d
make init
bash benchmark/run_benchmark.sh
```

Smoke test:

```bash
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

Official local profile:

```bash
for rate in 50 100 200; do
  docker compose down -v
  docker compose up -d
  sleep 30
  make init

  PYTHONUNBUFFERED=1 REQUEST_RATES=$rate N_RUNS=3 WARMUP_RUNS=1 WARMUP_SECS=10 \
  STABLE_MAX_WAIT=800 SHUFFLE_PARTITIONS=18 bash benchmark/run_benchmark.sh
done
```

## Outputs

```text
benchmark/results_YYYYMMDD_HHMMSS.csv
benchmark/staleness_YYYYMMDD_HHMMSS.csv
benchmark/benchmark_YYYYMMDD_HHMMSS.log
```

Primary metrics: `pipeline_e2e_s`, `processing_overhead_s`,
`first_gold_latency_s`, `avg_staleness_s`, `max_staleness_s`,
`row_integrity_ok`, `silver_to_bronze_ratio`, and `engine_ram_mb`.

## Staleness Methodology

`avg_staleness_s` in `results_*.csv` covers all samples from t=0, including
the pre-Gold-commit ramp-up. For the corrected (post-first-Gold) staleness
values used in the paper, run `benchmark_result/recompute_staleness.py` after
collecting results — it outputs `benchmark_result/staleness_corrected.csv`.

## Delta Row Count

`benchmark.py` uses an incremental Delta log scan (`_delta_file_map` cache)
that reads only log files with index greater than the last-scanned index.
This keeps the between-run overhead to O(new log files) rather than
O(all log files), which was the source of inflated inter-run gaps in earlier
versions of the harness.
