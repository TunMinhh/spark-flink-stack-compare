# Spark vs Flink Realtime Wearable Pipeline Benchmark

Benchmark of two Medallion-architecture pipelines on real LifeSnaps wearable
data (heart rate, HRV, breathing; 71 users; 1,500–6,000 events/s).

| | Pipeline A | Pipeline B |
|---|---|---|
| Engine | Spark Structured Streaming | Apache Flink |
| Table format | Delta Lake | Apache Iceberg |
| Storage | HDFS | HDFS |
| Catalog | Delta transaction log | Iceberg REST (PostgreSQL) |
| Trigger/checkpoint | 15 s trigger (all layers) | 5 s checkpoint |

Both pipelines share the same HDFS backend and follow the same three-layer
Medallion path:

```text
producer_realtime.py
  → Kafka intraday topics (HR, HRV, Breathing)
  → Bronze intraday tables
  → Silver intraday tables
  → Gold daily_intraday_summary
```

The AI training (`make train-ai`) and Grafana export (`make export-gold`) jobs
are available inside each pipeline but are **not** part of the benchmark
critical path.

## Headline Results (mean of 3 measured runs)

| Metric | Flink (B) | Spark (A) | Ratio |
|---|---|---|---|
| Gold-ready E2E (6,000 e/s) | **32.1 s** | 63.1 s | 2.0x |
| Bronze lag | **14.3 s** | 34.5 s | 2.4x |
| Silver lag | **19.4 s** | 48.4 s | 2.5x |
| Gold lag | **17.1 s** | 50.4 s | 3.0x |
| Avg Gold staleness | **12.5 s** | 45.5 s | 3.6x |
| Data integrity | 100% | 100% | - |

The staleness values above are taken from the checked-in result CSVs used to
render the current report figures.

The comparison uses the stable operating cadence for each stack: Spark
Structured Streaming runs with 15 s triggers, while Flink commits Iceberg
snapshots through 5 s checkpoints. Flink is faster across Bronze, Silver, and
Gold in this benchmark, with the largest user-visible difference appearing in
Gold freshness.

## Fairness Controls Applied

The benchmark uses these controls so both stacks process the same workload:

1. **Gold algorithm** — Both pipelines use stateful incremental aggregation
   for `daily_intraday_summary`.
2. **Stable cadence** — Spark uses 15 s triggers across Bronze/Silver/Gold;
   Flink uses 5 s checkpoints across the benchmark path.
3. **Small-file explosion** — Added `repartition(event_date)` to Bronze/Silver
   writes, matching Flink's Iceberg sink behaviour.
4. **Workload dating** — Both producers now tag events with the ingestion date
   (not the historical LifeSnaps sample date), collapsing the Gold key set to
   at most 71 groups per run.

## Repository Layout

```text
pipeline_a/           Spark pipeline, services, benchmark runner
pipeline_b/           Flink pipeline, services, benchmark runner
benchmark_result/     Saved results, figures, and metric dictionary
data/                 Shared CSV source files
```

### benchmark_result/

```text
spark_result/{50,100,200}_result.csv      Per-run metrics, Pipeline A
flink_result/{50,100,200}_result.csv      Per-run metrics, Pipeline B
figures/*.png                             Report/slide figures generated from results
README.md                                 Metric dictionary and benchmark plan
```

## Data Files

Place the shared CSV files in `data/` before running either pipeline:

```text
data/hourly_fitbit_sema_df_unprocessed.csv
data/daily_fitbit_sema_df_unprocessed.csv
```

Both pipelines read these as `../data/...`. Do not duplicate them inside
`pipeline_a/` or `pipeline_b/`.

## Quick Smoke Run

Run only one pipeline at a time — ports, container names, and available RAM
overlap between the two stacks.

**Spark (Pipeline A):**

```bash
cd pipeline_a
cp .env.example .env
docker compose up -d --build
make init
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
  bash benchmark/run_benchmark.sh
```

**Flink (Pipeline B):**

```bash
cd pipeline_b
cp .env.example .env
docker compose up -d --build
sleep 60
make init
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
  FLINK_CHECKPOINT_INTERVAL="5 s" FLINK_CHECKPOINT_INTERVAL_SECONDS=5 \
  GOLD_CHECKPOINT_SECONDS=5 PARALLELISM=6 bash benchmark/run_benchmark.sh
```

Pass criteria for both: `Bronze rows > 0`, `Silver rows > 0`,
`Gold refreshed = yes`, `row_integrity_ok = true`.

## Official Benchmark

Official runs reset state between load levels.

**Spark:**

```bash
for rate in 50 100 200; do
  docker compose down -v && docker compose up -d && sleep 30 && make init
  PYTHONUNBUFFERED=1 REQUEST_RATES=$rate N_RUNS=3 WARMUP_RUNS=1 WARMUP_SECS=10 \
    STABLE_MAX_WAIT=800 SHUFFLE_PARTITIONS=18 bash benchmark/run_benchmark.sh
done
```

**Flink:**

```bash
for rate in 50 100 200; do
  docker compose down -v && docker compose up -d && sleep 60 && make init
  PYTHONUNBUFFERED=1 REQUEST_RATES=$rate N_RUNS=3 WARMUP_RUNS=1 WARMUP_SECS=10 \
    STABLE_MAX_WAIT=800 FLINK_CHECKPOINT_INTERVAL="5 s" \
    FLINK_CHECKPOINT_INTERVAL_SECONDS=5 GOLD_CHECKPOINT_SECONDS=5 \
    PARALLELISM=6 bash benchmark/run_benchmark.sh
done
```

Rate mapping (default `DELAY=0.1`):

| `REQUEST_RATES` | Events/s |
|---|---|
| 50 | 1,500 |
| 100 | 3,000 |
| 200 | 6,000 |

## Saved Benchmark Files

The checked-in benchmark result set keeps the final per-run CSV files only:

```text
benchmark_result/spark_result/{50,100,200}_result.csv
benchmark_result/flink_result/{50,100,200}_result.csv
benchmark_result/figures/*.png
```

`is_warmup=true` rows are kept for traceability but excluded from reported
averages. The headline figures use the three measured runs per load level.

## Figures Used in the Report

```text
benchmark_result/figures/e2e.png
benchmark_result/figures/staleness.png
benchmark_result/figures/layer_lag.png
benchmark_result/figures/spark_flink_ratio.png
```

These figures reflect the current reported benchmark: Spark 15 s triggers and
Flink 5 s checkpoints.

## Notes

- `.env` is local; `.env.example` contains safe defaults.
- Pipeline B uses HDFS as the Iceberg warehouse for the benchmark path. MinIO
  is present only for MLflow artifact storage.
- For a clean serving database after schema changes, use `make reset-pg-sink`
  inside the selected pipeline.
- For long SSH sessions use `tmux` to avoid losing a benchmark run on disconnect.
