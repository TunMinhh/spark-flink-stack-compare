# Pipeline A: Spark Structured Streaming + Delta Lake

Pipeline A runs the realtime intraday path only:

```text
producer_realtime
-> Kafka intraday topics
-> Spark Bronze Delta intraday tables
-> Spark Silver Delta intraday tables
-> Spark Gold daily_intraday_summary
```

The producer reads baseline values from:

```text
../data/hourly_fitbit_sema_df_unprocessed.csv
../data/daily_fitbit_sema_df_unprocessed.csv
```

Those CSV files are used only to generate intraday events. They are not emitted
as hourly or daily Kafka topics.

## Run

```bash
cp .env.example .env
docker compose up -d --build
make init
bash benchmark/run_benchmark.sh
```

Smoke test:

```bash
PYTHONUNBUFFERED=1 REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 \
bash benchmark/run_benchmark.sh
```

## Jobs

```text
make producer-realtime
make bronze
make silver
make gold
make train-ai
make export-gold
```

`make train-ai` trains AttentionLSTM from Silver intraday tables and writes
`ai_intraday_insights` to Gold. `make export-gold` publishes
`daily_intraday_summary` and `ai_intraday_insights` to Postgres for Grafana.
