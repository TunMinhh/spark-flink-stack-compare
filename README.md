# Spark vs Flink Realtime Wearable Pipeline Benchmark

Benchmark of two Medallion-architecture pipelines on real LifeSnaps wearable
data (heart rate, HRV, breathing; 71 users; 1,500–6,000 events/s).

| | Pipeline A | Pipeline B |
|---|---|---|
| Engine | Spark Structured Streaming | Apache Flink |
| Table format | Delta Lake | Apache Iceberg |
| Storage | HDFS | HDFS |
| Catalog | Delta transaction log | Iceberg REST (PostgreSQL) |
| Trigger/checkpoint | 15 s (all layers) | 15 s checkpoint |

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
| End-to-end latency (6,000 e/s) | **58 s** | 116 s | 2.0× |
| Bronze lag | **18 s** | 86 s | 4.7× |
| Silver lag | **31 s** | 103 s | 3.4× |
| Gold lag | **45 s** | 51 s | 1.1× |
| Avg Gold staleness† | **20 s** | 52 s | 2.6× |
| Data integrity | 100% | 100% | — |

†Staleness measured from the first Gold commit of each run
(see `benchmark_result/recompute_staleness.py`).

The gap lives entirely in the ingest layers. Once the Gold algorithm and
cadence are equalised, both stacks commit within **1.1–1.3×** of each other at
the aggregation layer.

## Fairness Controls Applied

Before measuring, four confounds were removed:

1. **Gold algorithm** — Spark Gold changed from stateless full-table re-scan
   to the same stateful incremental aggregation Flink uses.
2. **Trigger cadence** — All Spark triggers lowered to 15 s to match Flink's
   checkpoint interval.
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
generate_figures.py   Produces fig1_e2e_latency.png, fig1b_staleness.png,
                      fig2_layer_lag.png from the saved result CSVs
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

## Generating Figures

```bash
python3 generate_figures.py
```

Writes to `/mnt/c/Users/tranm/Downloads/`:

```text
fig1_e2e_latency.png   End-to-end latency by load level
fig1b_staleness.png    Corrected avg Gold staleness by load level
fig2_layer_lag.png     Per-layer catch-up lag (Bronze/Silver/Gold)
```

Staleness in `fig1b_staleness.png` uses values from `staleness_corrected.csv`.

## Notes

- `.env` is local; `.env.example` contains safe defaults.
- Pipeline B uses HDFS as the Iceberg warehouse for the benchmark path. MinIO
  is present only for MLflow artifact storage.
- For a clean serving database after schema changes, use `make reset-pg-sink`
  inside the selected pipeline.
- For long SSH sessions use `tmux` to avoid losing a benchmark run on disconnect.
