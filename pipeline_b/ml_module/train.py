"""
AI training pipeline - Silver intraday Iceberg -> AttentionLSTM -> MLflow + Gold.
"""

from __future__ import annotations

import os
from datetime import datetime

import mlflow
import mlflow.pytorch
import numpy as np
import pandas as pd
import pyarrow as pa
import torch
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import FloatType, NestedField, StringType, TimestampType
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

from model import AttentionLSTM, reconstruction_error


ICEBERG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")
SILVER_NS = os.getenv("ICEBERG_SILVER_NS", "silver")
GOLD_NS = os.getenv("ICEBERG_GOLD_NS", "gold")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "intraday-anomaly-detector")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "intraday-attention-lstm")

SEQ_LEN = int(os.getenv("AI_SEQ_LEN", "30"))
MIN_WINDOWS = int(os.getenv("AI_MIN_WINDOWS", "50"))
HIDDEN_SIZE = int(os.getenv("AI_HIDDEN_SIZE", "64"))
NUM_LAYERS = int(os.getenv("AI_NUM_LAYERS", "2"))
BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "64"))
EPOCHS = int(os.getenv("EPOCHS", "15"))
LR = float(os.getenv("AI_LR", "0.001"))

THRESH_GREEN = float(os.getenv("AI_THRESH_GREEN", "1.04"))
THRESH_YELLOW = float(os.getenv("AI_THRESH_YELLOW", "1.29"))
THRESH_RED = float(os.getenv("AI_THRESH_RED", "1.65"))

FEATURE_COLS = ["bpm", "rmssd", "breaths_per_minute"]


def _catalog():
    return load_catalog(
        "rest",
        **{
            "uri": ICEBERG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": AWS_KEY,
            "s3.secret-access-key": AWS_SECRET,
            "s3.path-style-access": "true",
        },
    )


def build_feature_matrix(cat) -> pd.DataFrame:
    hr = (
        cat.load_table(f"{SILVER_NS}.heart_rate_intraday")
        .scan(selected_fields=("user_id", "event_date", "event_timestamp", "bpm"))
        .to_pandas()
    )
    hrv = (
        cat.load_table(f"{SILVER_NS}.hrv_intraday")
        .scan(selected_fields=("user_id", "event_date", "event_timestamp", "rmssd"))
        .to_pandas()
    )
    breathing = (
        cat.load_table(f"{SILVER_NS}.breathing_intraday")
        .scan(selected_fields=("user_id", "event_date", "event_timestamp", "breaths_per_minute"))
        .to_pandas()
    )
    return (
        hr.merge(hrv, on=["user_id", "event_date", "event_timestamp"], how="outer")
        .merge(breathing, on=["user_id", "event_date", "event_timestamp"], how="outer")
        .dropna(subset=["user_id", "event_timestamp"])
        .sort_values(["user_id", "event_timestamp"])
        .reset_index(drop=True)
    )


def build_sequences(df: pd.DataFrame, scaler: StandardScaler | None = None):
    df[FEATURE_COLS] = df.groupby("user_id")[FEATURE_COLS].ffill().bfill()
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(df[FEATURE_COLS].mean(numeric_only=True))
    df = df.dropna(subset=FEATURE_COLS)

    if scaler is None:
        scaler = StandardScaler().fit(df[FEATURE_COLS].values)
    df[FEATURE_COLS] = scaler.transform(df[FEATURE_COLS].values)

    x_rows, index_rows = [], []
    for user_id, group in df.groupby("user_id"):
        values = group[FEATURE_COLS].values
        dates = group["event_date"].tolist()
        timestamps = group["event_timestamp"].tolist()
        for i in range(SEQ_LEN, len(group) + 1):
            x_rows.append(values[i - SEQ_LEN : i])
            index_rows.append((user_id, dates[i - 1], timestamps[i - 1]))
    return np.asarray(x_rows, dtype=np.float32), index_rows, scaler


def train_model(x: np.ndarray) -> tuple[AttentionLSTM, list[float]]:
    tensor = torch.from_numpy(x)
    loader = DataLoader(TensorDataset(tensor), batch_size=BATCH_SIZE, shuffle=True)
    model = AttentionLSTM(input_size=len(FEATURE_COLS), hidden_size=HIDDEN_SIZE, num_layers=NUM_LAYERS)
    opt = torch.optim.Adam(model.parameters(), lr=LR)
    loss_fn = torch.nn.MSELoss()
    losses = []
    for epoch in range(EPOCHS):
        model.train()
        running = 0.0
        for (batch,) in loader:
            opt.zero_grad()
            recon, _ = model(batch)
            loss = loss_fn(recon, batch)
            loss.backward()
            opt.step()
            running += loss.item() * batch.size(0)
        avg = running / len(tensor)
        losses.append(avg)
        print(f"[train] epoch {epoch + 1:02d}/{EPOCHS} loss={avg:.5f}")
    return model, losses


def score_anomalies(model: AttentionLSTM, x: np.ndarray) -> np.ndarray:
    model.eval()
    tensor = torch.from_numpy(x)
    with torch.no_grad():
        recon, _ = model(tensor)
        return reconstruction_error(tensor, recon).numpy()


def classify(z_scores: np.ndarray) -> list[str]:
    labels = []
    for z in z_scores:
        if z >= THRESH_RED:
            labels.append("red")
        elif z >= THRESH_YELLOW:
            labels.append("yellow")
        elif z >= THRESH_GREEN:
            labels.append("green")
        else:
            labels.append("normal")
    return labels


_SCHEMA = Schema(
    NestedField(1, "user_id", StringType(), required=False),
    NestedField(2, "event_date", StringType(), required=False),
    NestedField(3, "event_timestamp", TimestampType(), required=False),
    NestedField(4, "reconstruction_error", FloatType(), required=False),
    NestedField(5, "z_score", FloatType(), required=False),
    NestedField(6, "severity", StringType(), required=False),
    NestedField(7, "mlflow_run_id", StringType(), required=False),
)


def write_ai_insights(cat, index_rows, errs, z_scores, labels, run_id: str) -> None:
    try:
        cat.create_namespace(GOLD_NS)
    except NamespaceAlreadyExistsError:
        pass
    name = f"{GOLD_NS}.ai_intraday_insights"
    try:
        table = cat.create_table(name, schema=_SCHEMA)
    except Exception:
        table = cat.load_table(name)

    rows = pd.DataFrame([
        {
            "user_id": uid,
            "event_date": str(event_date),
            "event_timestamp": pd.Timestamp(event_ts).to_pydatetime(),
            "reconstruction_error": float(err),
            "z_score": float(z),
            "severity": label,
            "mlflow_run_id": run_id,
        }
        for (uid, event_date, event_ts), err, z, label in zip(index_rows, errs, z_scores, labels)
    ])
    arrow_table = pa.Table.from_pandas(rows, schema=pa.schema([
        pa.field("user_id", pa.string()),
        pa.field("event_date", pa.string()),
        pa.field("event_timestamp", pa.timestamp("us")),
        pa.field("reconstruction_error", pa.float32()),
        pa.field("z_score", pa.float32()),
        pa.field("severity", pa.string()),
        pa.field("mlflow_run_id", pa.string()),
    ]), preserve_index=False)
    table.overwrite(arrow_table)
    print(f"[gold] wrote {len(rows)} ai_intraday_insights rows to Iceberg")


def main() -> str:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    cat = _catalog()
    pdf = build_feature_matrix(cat)
    print(f"[data] loaded {len(pdf)} intraday rows")
    x, index_rows, _scaler = build_sequences(pdf)
    print(f"[data] built {len(x)} windows of shape {x.shape[1:] if len(x) else (SEQ_LEN, len(FEATURE_COLS))}")
    if len(x) < MIN_WINDOWS:
        raise RuntimeError(f"Only {len(x)} training windows; need at least {MIN_WINDOWS}.")

    with mlflow.start_run() as run:
        mlflow.log_params({
            "source": "silver_intraday",
            "seq_len": SEQ_LEN,
            "hidden_size": HIDDEN_SIZE,
            "num_layers": NUM_LAYERS,
            "epochs": EPOCHS,
            "batch_size": BATCH_SIZE,
            "lr": LR,
            "n_features": len(FEATURE_COLS),
            "n_windows": len(x),
        })
        model, losses = train_model(x)
        mlflow.log_metric("final_train_loss", losses[-1])
        for i, loss in enumerate(losses):
            mlflow.log_metric("train_loss", loss, step=i)
        errs = score_anomalies(model, x)
        z = (errs - errs.mean()) / (errs.std() + 1e-8)
        labels = classify(z)
        mlflow.log_metric("anomaly_rate_red", float((np.array(labels) == "red").mean()))
        mlflow.log_metric("anomaly_rate_yellow", float((np.array(labels) == "yellow").mean()))
        mlflow.log_metric("anomaly_rate_green", float((np.array(labels) == "green").mean()))
        mlflow.pytorch.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)
        write_ai_insights(cat, index_rows, errs, z, labels, run.info.run_id)
        print(f"[done] MLflow run_id={run.info.run_id}")
        return run.info.run_id


if __name__ == "__main__":
    main()
