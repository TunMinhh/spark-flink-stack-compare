# Data Pipeline Comparison Benchmark

This repository compares two streaming data pipeline implementations over the same wearable realtime workload:

- **Pipeline A**: Spark Structured Streaming + Delta Lake + HDFS
- **Pipeline B**: Apache Flink + Apache Iceberg + MinIO/S3

The benchmark is designed to answer one practical question: under the same producer workload, how long does each pipeline take to ingest realtime wearable events, transform them through Bronze/Silver, and make refreshed Gold output available?

## What Is Being Benchmarked

The measured workload is the realtime intraday path:

```text
producer_realtime.py
  -> Kafka realtime topics
  -> Bronze intraday tables
  -> Silver intraday tables
  -> Gold daily_intraday_summary table
```

The producer emits three realtime signal families per tick: heart rate intraday, HRV intraday, and breathing intraday. For example, `REQUEST_RATES=200` means 200 users per tick, which is approximately `200 * 3 / 0.1 = 6000` offered events per second when `DELAY=0.1`.

Benchmark runs use a fixed tick count by default: `MAX_TICKS = WARMUP_SECS / DELAY`. With the default `WARMUP_SECS=10` and `DELAY=0.1`, each run emits 100 ticks. This keeps row counts comparable across Spark and Flink even if producer wall-clock speed varies.

Bronze/Silver row counts and stable checks are measured across all three intraday tables: `heart_rate_intraday`, `hrv_intraday`, and `breathing_intraday`. Bronze and Silver are only considered stable after the combined row delta across all three tables reaches `expected_rows` (`users_per_tick × 3 × MAX_TICKS`) and the total row count stops changing for several consecutive polls.

Both benchmark scripts run as continuous streaming benchmarks. They do **not** reset table state between warmup and measured runs, and they do **not** reset state when moving from one request rate to the next. At the start of each run, the benchmark records the current Bronze/Silver/Gold row counts as the run baseline; the reported `rows_added_*` metrics are deltas from that baseline. This means the benchmark measures not only fresh-burst latency, but also how each pipeline behaves as table history, metadata, and streaming state accumulate over a long-running session.

The benchmark does **not** measure the whole project end to end. It does not include dashboards, ML jobs, historical backfill jobs, or static daily/hourly pre-seeding. Those pieces may exist in the pipelines, but the official benchmark focuses on the streaming path above.

The default Compose profile starts the realtime processing stack, MLflow/FastAPI AI training, and the Postgres/Grafana exposition layer.

## Current Default Profile

| Setting | Pipeline A: Spark | Pipeline B: Flink |
| --- | ---: | ---: |
| Request rates | `50,100,200` users/tick | `50,100,200` users/tick |
| Approx offered load | `1500,3000,6000` events/s | `1500,3000,6000` events/s |
| Measured runs per rate | `3` | `3` |
| Warmup runs per rate | `1` | `1` |
| Producer burst duration | `10s` | `10s` |
| Reset between runs/rates | No | No |
| Trigger / checkpoint interval | `30s` Spark triggers | `15s` Flink checkpoints |
| Stable wait timeout | `800s` | `800s` |
| Kafka partitions | `12` | `12` |
| Main parallelism knob | `SHUFFLE_PARTITIONS=18` | `PARALLELISM=6` |
| Storage format | Delta Lake | Iceberg |
| Storage backend | HDFS | MinIO/S3 |

`SHUFFLE_PARTITIONS` and `PARALLELISM` are not mathematically equivalent. Spark's shuffle partitions control how many output tasks are used for shuffle-heavy stages. Flink's parallelism controls operator subtasks. They are tuning knobs for different execution models, so compare observed results, not the raw numbers.

## How To Run

Run only one pipeline stack at a time unless you have intentionally changed ports and resource limits.

### Pipeline A: Spark

```bash
cd pipeline_a
docker compose up -d
make init
PYTHONUNBUFFERED=1 bash benchmark/run_benchmark.sh
```

Quick one-rate smoke benchmark:

```bash
cd pipeline_a
PYTHONUNBUFFERED=1 \
REQUEST_RATES=200 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

The Spark benchmark runner checks the required containers, creates HDFS paths and Kafka topics, and starts missing Bronze/Silver/Gold streaming jobs.

### Pipeline B: Flink

```bash
cd pipeline_b
docker compose up -d
make init
PYTHONUNBUFFERED=1 bash benchmark/run_benchmark.sh
```

Quick one-rate smoke benchmark:

```bash
cd pipeline_b
PYTHONUNBUFFERED=1 \
REQUEST_RATES=200 \
N_RUNS=1 \
WARMUP_RUNS=0 \
WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

The Flink benchmark runner checks the required containers, creates Kafka topics and Iceberg namespaces, and starts missing Bronze/Silver/Gold jobs. It does not pre-seed static CSV data for the official benchmark workload.

## Resetting State

For an official comparison, start each pipeline from a clean state so old table data and checkpoints do not affect the next result.

Pipeline A clean reset:

```bash
cd pipeline_a
docker compose down -v
docker compose up -d
make init
```

Pipeline B clean reset:

```bash
cd pipeline_b
make cancel-all
make reset
make init
```

Do not reset between warmup and measured runs inside one official benchmark session unless you intentionally change both pipelines to use the same reset policy. The default methodology is continuous: baselines increase from run to run, and each run measures the delta added after its own baseline.

Resetting matters most when comparing separate official benchmark sessions. For example, reset both stacks before starting a new Spark-vs-Flink result set, after changing benchmark code, after changing table schemas, or after a failed run that left jobs/checkpoints in an uncertain state.

The Spark benchmark does not run Delta `OPTIMIZE` inside the benchmark loop. Compaction is a maintenance workload and can distort streaming latency if it competes with Bronze/Silver/Gold jobs during measurement.

## Output Files

Each benchmark writes timestamped files under that pipeline's `benchmark/` directory:

- `results_YYYYMMDD_HHMMSS.csv`: one row per run, including warmup rows.
- `staleness_YYYYMMDD_HHMMSS.csv`: sampled Gold freshness during each run.
- `benchmark_YYYYMMDD_HHMMSS.log`: terminal log from the wrapper script.

When comparing results, filter out `is_warmup=true`.

## Metrics

| Metric | Meaning |
| --- | --- |
| `req_per_sec` | Theoretical offered producer rate based on users per tick, signal count, and producer delay. |
| `producer_stop_s` | How long the producer actually ran before it was stopped. |
| `target_ticks` | Number of producer ticks requested for the run. Defaults to `WARMUP_SECS / DELAY`. |
| `expected_rows` | Expected total intraday rows: `users_per_tick * 3 * target_ticks`. |
| `producer_actual_rps` | Actual total Bronze intraday rows committed divided by producer runtime. This is useful when the producer or Kafka cannot fully sustain the theoretical offered rate. |
| `gold_e2e_s` | Main end-to-end metric: producer start to the full measured path becoming stable after the burst. Bronze, Silver, and Gold are watched in parallel; the reported E2E is the latest stable time among them. Lower is better. |
| `pipeline_e2e_s` | Alias for the full measured-path E2E. Added to make the E2E definition explicit while keeping `gold_e2e_s` for backward-compatible summaries. |
| `first_gold_latency_s` | Producer start to the first observed Gold commit. This shows how quickly the pipeline starts producing visible output. |
| `processing_overhead_s` | Time from producer stop until the full measured path is stable. |
| `catchup_ratio` | `processing_overhead_s / warmup_secs`. A value above `1.0` means the pipeline needed longer to catch up than the input burst duration. |
| `avg_staleness_s` | Average age of Gold output during the run. Lower means fresher analytical output. |
| `max_staleness_s` | Worst observed Gold output age during the run. This captures freshness spikes. |
| `bronze_lag_s` | Time from producer stop until Bronze reaches expected rows and stabilizes. |
| `silver_lag_s` | Time from producer stop until Silver reaches expected rows and stabilizes. |
| `gold_lag_s` | Time from producer stop until Gold stabilizes. |
| `bronze_throughput_rps` | Total Bronze intraday rows committed per producer burst second. |
| `silver_throughput_rps` | Total Silver intraday rows committed per producer burst second. |
| `rows_added_bronze` | Total Bronze intraday rows added during this run after subtracting baseline rows. |
| `rows_added_silver` | Total Silver intraday rows added during this run after subtracting baseline rows. |
| `rows_added_gold` | Gold row-count delta. This can be small or zero for aggregate/upsert outputs even when Gold refreshed correctly. |
| `silver_to_bronze_ratio` | `rows_added_silver / rows_added_bronze`. Use this as an integrity check; unexpected drops mean the two runs may not be processing comparable data. |
| `bronze_expected_ok`, `silver_expected_ok`, `row_integrity_ok` | Whether Bronze/Silver row deltas exactly match `expected_rows`. Runs with `row_integrity_ok=false` should not be used for headline comparison. |
| `gold_refreshed` | Whether Gold produced a new commit/snapshot after the run baseline. |
| `bronze_ok`, `silver_ok`, `gold_ok` | Whether each layer reached the benchmark's stable condition before timeout. |
| `engine_ram_mb` | Peak RAM sampled from the main worker engine container. |
| `coord_ram_mb` | Peak RAM sampled from the coordinator/master container. |

## How To Read Results

Use `gold_e2e_s`, `avg_staleness_s`, `max_staleness_s`, and `first_gold_latency_s` as the primary user-visible performance metrics.

Use `rows_added_bronze`, `rows_added_silver`, `producer_actual_rps`, and `silver_to_bronze_ratio` as sanity checks. If one pipeline processed far fewer rows, the E2E number may look faster but the run is not comparable.

Use `catchup_ratio` to understand backpressure. A low E2E can still hide trouble if the catch-up ratio grows quickly as request rate increases.

Use `engine_ram_mb` as a resource signal, not as a complete cost metric. CPU, disk IO, object storage latency, JVM GC, checkpoint size, and network IO can also matter.

## Known Non-Equivalences

The benchmark is intentionally aligned, but the two systems are not identical.

- Spark Structured Streaming is micro-batch oriented; Flink is continuous streaming oriented.
- Delta Lake commits on HDFS and Iceberg snapshots on MinIO/S3 have different commit paths and metadata costs.
- Spark `SHUFFLE_PARTITIONS=18` is not the same thing as Flink `PARALLELISM=6`.
- Gold is an aggregate table. Row count may not increase even when aggregate values refresh.
- Bronze/Silver stable checks require the combined row delta to reach `expected_rows` and for row counts to stop changing. Gold stable checks rely on a newer commit/snapshot because aggregate row counts may stay unchanged.
- Neither benchmark resets state between runs or request-rate tiers by default. This is intentional for the continuous streaming profile, but it also means metadata/history accumulation is part of what is being measured.
- Spark in-benchmark Delta compaction is disabled. If compaction is needed for maintenance, run it outside official benchmark measurement windows.
- The benchmark measures committed table output, not internal operator latency.
- The theoretical offered rate can differ from actual input throughput. Always check `producer_actual_rps`.

Because of these differences, the safest claim is not "Flink is X times faster than Spark in general" or "Spark is X times slower than Flink in general." The safer claim is: under this repository's realtime wearable workload, data model, container sizing, and benchmark settings, one implementation produced fresher Gold output with the measured latency, throughput, and resource profile shown in the CSV results.

## Suggested Reporting Text

> We benchmarked two implementations of the same realtime wearable analytics workload: Spark Structured Streaming with Delta Lake/HDFS and Flink with Iceberg/MinIO. Each run emits the same Kafka realtime workload, then measures how long Bronze, Silver, and Gold outputs take to commit and stabilize. Warmup runs are excluded from reported results. Because Spark and Flink use different execution and table-commit models, results should be interpreted as end-to-end workload measurements rather than identical operator-level measurements.
