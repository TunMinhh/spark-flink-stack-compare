"""
AI training pipeline - Silver intraday -> AttentionLSTM -> MLflow + Gold.

This trains directly from producer_realtime output:
  Silver heart_rate_intraday + hrv_intraday + breathing_intraday
  -> per-user timestamp sequences
  -> AttentionLSTM autoencoder
  -> Gold ai_intraday_insights
"""

from __future__ import annotations

import os

import mlflow
import mlflow.pytorch
import numpy as np
import torch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader, TensorDataset

from model import AttentionLSTM, reconstruction_error


HDFS_SILVER = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
HDFS_GOLD = os.getenv("HDFS_GOLD_BASE", "hdfs://namenode:9000/data/gold/wearable")
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


def make_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("IntradayAITrain")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def build_feature_matrix(spark: SparkSession):
    hr = (
        spark.read.format("delta").load(f"{HDFS_SILVER}/heart_rate_intraday")
        .select("user_id", "event_date", "event_timestamp", "bpm")
    )
    hrv = (
        spark.read.format("delta").load(f"{HDFS_SILVER}/hrv_intraday")
        .select("user_id", "event_date", "event_timestamp", "rmssd")
    )
    breathing = (
        spark.read.format("delta").load(f"{HDFS_SILVER}/breathing_intraday")
        .select("user_id", "event_date", "event_timestamp", "breaths_per_minute")
    )

    joined = (
        hr.join(hrv, on=["user_id", "event_date", "event_timestamp"], how="outer")
        .join(breathing, on=["user_id", "event_date", "event_timestamp"], how="outer")
        .dropna(subset=["user_id", "event_timestamp"])
        .orderBy("user_id", "event_timestamp")
    )
    return joined.toPandas()


def build_sequences(df, scaler: StandardScaler | None = None):
    df = df.sort_values(["user_id", "event_timestamp"]).reset_index(drop=True)
    df[FEATURE_COLS] = df.groupby("user_id")[FEATURE_COLS].ffill().bfill()
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(df[FEATURE_COLS].mean(numeric_only=True))
    df = df.dropna(subset=FEATURE_COLS)

    if scaler is None:
        scaler = StandardScaler().fit(df[FEATURE_COLS].values)
    df[FEATURE_COLS] = scaler.transform(df[FEATURE_COLS].values)

    x_rows, index_rows = [], []
    for user_id, group in df.groupby("user_id"):
        values = group[FEATURE_COLS].values
        timestamps = group["event_timestamp"].tolist()
        dates = group["event_date"].tolist()
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


def write_ai_insights(spark: SparkSession, index_rows, errs, z_scores, labels, run_id: str) -> None:
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("event_date", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("reconstruction_error", FloatType(), False),
        StructField("z_score", FloatType(), False),
        StructField("severity", StringType(), False),
        StructField("mlflow_run_id", StringType(), False),
    ])
    rows = [
        (uid, str(event_date), event_ts, float(err), float(z), label, run_id)
        for (uid, event_date, event_ts), err, z, label in zip(index_rows, errs, z_scores, labels)
    ]
    df = spark.createDataFrame(rows, schema=schema)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(f"{HDFS_GOLD}/ai_intraday_insights")
    )
    print(f"[gold] wrote {len(rows)} ai_intraday_insights rows")


def main() -> str:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    spark = make_spark()
    try:
        pdf = build_feature_matrix(spark)
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

            write_ai_insights(spark, index_rows, errs, z, labels, run.info.run_id)
            print(f"[done] MLflow run_id={run.info.run_id}")
            return run.info.run_id
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
