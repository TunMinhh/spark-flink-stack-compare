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

Staleness is measured after the first Gold commit of each run
(see `benchmark_result/recompute_staleness.py`).

The comparison uses the stable operating cadence for each stack: Spark
Structured Streaming runs with 15 s triggers, while Flink commits Iceberg
snapshots through 5 s checkpoints. Flink is faster across Bronze, Silver, and
Gold in this benchmark, with the largest user-visible difference appearing in
Gold freshness.

## Fairness Controls Applied

Before measuring, four confounds were removed:

1. **Gold algorithm** — Spark Gold changed from stateless full-table re-scan
   to the same stateful incremental aggregation Flink uses.
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
benchmark_result/     Saved results, metric dictionary, analysis scripts
data/                 Shared CSV source files
```

### benchmark_result/

```text
spark_result/{50,100,200}_result.csv      Per-run metrics, Pipeline A
spark_result/{50,100,200}_staleness.csv   Raw staleness samples, Pipeline A
flink_result/{50,100,200}_result.csv      Per-run metrics, Pipeline B
flink_result/{50,100,200}_staleness.csv   Raw staleness samples, Pipeline B
staleness_corrected.csv                   Corrected staleness (post-first-Gold)
recompute_staleness.py                    Script to recompute corrected staleness
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
  PARALLELISM=6 bash benchmark/run_benchmark.sh
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
    STABLE_MAX_WAIT=800 PARALLELISM=6 bash benchmark/run_benchmark.sh
done
```

Rate mapping (default `DELAY=0.1`):

| `REQUEST_RATES` | Events/s |
|---|---|
| 50 | 1,500 |
| 100 | 3,000 |
| 200 | 6,000 |

## Recomputing Corrected Staleness

After collecting results, run:

```bash
cd benchmark_result
python3 recompute_staleness.py
```

This reads `staleness_*.csv` and `results_*.csv` from `spark_result/` and
`flink_result/`, filters each run's staleness samples to those at or after
the first Gold commit (`wall_s >= first_gold_latency_s`), and writes
`staleness_corrected.csv` with per-run corrected avg/max/min values.

The raw `avg_staleness_s` in `results_*.csv` covers all samples from t=0 and
is inflated by the inter-run idle gap; the corrected values are the ones
reported in the paper.

## Figures Used in the Report

```text
benchmark_result/_pptx_preview/e2e.png
benchmark_result/_pptx_preview/staleness.png
benchmark_result/_pptx_preview/layer_lag.png
benchmark_result/_pptx_preview/slide11_ratio.png
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
