# Spark vs Flink Realtime Wearable Pipeline

This repository contains two comparable realtime data pipelines for wearable
intraday signals.

- `pipeline_a/`: Spark Structured Streaming + Delta Lake + HDFS
- `pipeline_b/`: Apache Flink + Apache Iceberg + HDFS

Both pipelines ingest the same `producer_realtime.py` workload and process only
the realtime intraday path:

```text
producer_realtime.py
-> Kafka intraday topics
-> Bronze intraday tables
-> Silver intraday tables
-> Gold daily_intraday_summary
-> AttentionLSTM ai_intraday_insights
-> PostgreSQL sink for Grafana
```

The old daily/hourly topic pipelines are intentionally not part of the active
path. The hourly and daily CSV files are used only as baseline source data for
the realtime producer.

## Repository Layout

```text
data/                 Shared CSV input files
pipeline_a/           Spark pipeline, services, benchmark runner, ML module
pipeline_b/           Flink pipeline, services, benchmark runner, ML module
benchmark_result/     Benchmark plan, metric dictionary, and saved results
```

## Data Files

Place the shared CSV files in the repository-level `data/` directory:

```text
data/hourly_fitbit_sema_df_unprocessed.csv
data/daily_fitbit_sema_df_unprocessed.csv
```

Both pipelines read these files through `../data/...`. Do not copy duplicate CSV
files into `pipeline_a/` or `pipeline_b/`.

## Where To Start

Use the pipeline-specific README files for setup and operation:

- `pipeline_a/README.md` for Spark
- `pipeline_b/README.md` for Flink
- `pipeline_a/ml_module/README.md` and `pipeline_b/ml_module/README.md` for the
  AttentionLSTM training flow

Use `benchmark_result/README.md` for the official benchmark plan, fairness
contract, result-file meanings, and metric formulas.

## Quick Smoke Run

Run only one pipeline at a time on the same VM because ports, container names,
and available RAM overlap.

Spark:

```bash
cd pipeline_a
cp .env.example .env
docker compose up -d --build
make init
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
  bash benchmark/run_benchmark.sh
```

Flink:

```bash
cd pipeline_b
cp .env.example .env
docker compose up -d --build
make init
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
  bash benchmark/run_benchmark.sh
```

## Notes

- `.env` is local and should not be committed; `.env.example` contains safe
  placeholders/defaults.
- `data/*.csv` is intended to be committed if the benchmark dataset should
  travel with the repo.
- Pipeline B uses HDFS as the Iceberg warehouse for the benchmark path. MinIO
  remains only for MLflow artifact storage.
- For a clean serving database after schema changes or old runs, use
  `make reset-pg-sink` inside the selected pipeline.
