# Pipeline A Machine Learning Module

This module trains and serves the AttentionLSTM anomaly model for the Spark /
Delta pipeline.

```text
Silver Delta intraday tables
  heart_rate_intraday
  hrv_intraday
  breathing_intraday
-> joined per user and event timestamp
-> AttentionLSTM autoencoder
-> MLflow model registry
-> Gold Delta ai_intraday_insights
```

## Files

| File | Purpose |
| --- | --- |
| `train.py` | Loads Silver intraday Delta data from HDFS, trains AttentionLSTM, logs to MLflow, and writes Gold AI insights. |
| `serve.py` | FastAPI service exposing `/train`, `/train/{job_id}`, `/predict`, and `/health`. |
| `model/attention_lstm.py` | PyTorch AttentionLSTM autoencoder and reconstruction error helper. |
| `requirements.txt` | Python dependencies for the FastAPI/ML container. |
| `Dockerfile` | Builds the ML service image used by docker compose. |

## Data Contract

Training reads these Silver Delta tables from `HDFS_SILVER_BASE`:

| Table | Required columns |
| --- | --- |
| `heart_rate_intraday` | `user_id`, `event_date`, `event_timestamp`, `bpm` |
| `hrv_intraday` | `user_id`, `event_date`, `event_timestamp`, `rmssd` |
| `breathing_intraday` | `user_id`, `event_date`, `event_timestamp`, `breaths_per_minute` |

The model features are:

```text
bpm, rmssd, breaths_per_minute
```

Rows are inner-joined on `user_id`, `event_date`, and `event_timestamp`, sorted
per user, then converted into sliding windows of length `AI_SEQ_LEN`.

## Output

Training writes Gold Delta table:

```text
HDFS_GOLD_BASE/ai_intraday_insights
```

The table contains per-window anomaly scores and status bands derived from the
AttentionLSTM reconstruction error. This table is exported to Postgres together
with `daily_intraday_summary` for dashboard use.

## Run

Start the default stack from `pipeline_a`:

```bash
docker compose up -d
```

Train through FastAPI:

```bash
make train-ai
```

Or call the service directly:

```bash
curl -sf -X POST http://localhost:8000/train | python3 -m json.tool
curl -sf http://localhost:8000/train/<job_id> | python3 -m json.tool
```

## Configuration

The compose service reads these values from `.env`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MLFLOW_EXPERIMENT` | `intraday-anomaly-detector` | MLflow experiment name. |
| `MLFLOW_MODEL_NAME` | `intraday-attention-lstm` | Registered model name. |
| `AI_SEQ_LEN` | `30` | Timesteps per training window. |
| `AI_MIN_WINDOWS` | `50` | Minimum windows required before training proceeds. |
| `AI_HIDDEN_SIZE` | `64` | LSTM hidden size. |
| `AI_NUM_LAYERS` | `2` | LSTM layer count. |
| `AI_BATCH_SIZE` | `64` | Training batch size. |
| `AI_LR` | `0.001` | Learning rate. |
| `EPOCHS` | `15` | Training epochs. |
| `AI_THRESH_GREEN` | `1.04` | Low anomaly band threshold. |
| `AI_THRESH_YELLOW` | `1.29` | Medium anomaly band threshold. |
| `AI_THRESH_RED` | `1.65` | High anomaly band threshold. |

## Notes

This model is trained from Silver intraday data, not directly from Kafka. That
keeps training reproducible while still using data produced by the realtime
pipeline.
