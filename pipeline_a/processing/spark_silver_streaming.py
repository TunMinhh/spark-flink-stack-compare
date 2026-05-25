"""
SILVER LAYER - Spark Structured Streaming: Bronze Delta -> Silver Delta.

This job is scoped to the high-volume intraday benchmark path:
heart_rate_intraday, hrv_intraday, and breathing_intraday. It runs
continuously and appends cleaned rows to Silver Delta tables.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType


HDFS_BRONZE_BASE = os.getenv("HDFS_BRONZE_BASE", "hdfs://namenode:9000/data/bronze/wearable")
HDFS_SILVER_BASE = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/silver/wearable")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "18")
TRIGGER_SECONDS = os.getenv("SILVER_TRIGGER_SECONDS", "15")
STARTUP_WAIT_SECONDS = int(os.getenv("STARTUP_WAIT_SECONDS", "300"))
SILVER_WRITE_COALESCE = int(os.getenv("SILVER_WRITE_COALESCE", "1"))

SILVER_ONLY = {
    name.strip()
    for name in os.getenv(
        "SILVER_ONLY",
        "heart_rate_intraday,hrv_intraday,breathing_intraday",
    ).split(",")
    if name.strip()
}


spark = (
    SparkSession.builder.appName("WearableSilverStreamingIntraday")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


SCHEMAS = {
    "heart_rate_intraday": StructType([StructField("bpm", FloatType())]),
    "hrv_intraday": StructType([StructField("rmssd", FloatType())]),
    "breathing_intraday": StructType([StructField("breaths_per_minute", FloatType())]),
}

SILVER_SCHEMAS = {
    "heart_rate_intraday": StructType([
        StructField("user_id", StringType()),
        StructField("kafka_ingest_time", TimestampType()),
        StructField("event_date", DateType()),
        StructField("event_hour", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
        StructField("bpm", FloatType()),
        StructField("processed_at", TimestampType()),
    ]),
    "hrv_intraday": StructType([
        StructField("user_id", StringType()),
        StructField("kafka_ingest_time", TimestampType()),
        StructField("event_date", DateType()),
        StructField("event_hour", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
        StructField("rmssd", FloatType()),
        StructField("processed_at", TimestampType()),
    ]),
    "breathing_intraday": StructType([
        StructField("user_id", StringType()),
        StructField("kafka_ingest_time", TimestampType()),
        StructField("event_date", DateType()),
        StructField("event_hour", IntegerType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
        StructField("breaths_per_minute", FloatType()),
        StructField("processed_at", TimestampType()),
    ]),
}


def wait_for_delta_table(path: str, label: str) -> None:
    start = time.time()
    while time.time() - start < STARTUP_WAIT_SECONDS:
        try:
            spark.read.format("delta").load(path).schema
            return
        except Exception as exc:
            msg = str(exc).splitlines()[0]
        print(f"[silver-stream] Waiting for Bronze/{label} at {path} ...")
        if msg:
            print(f"[silver-stream]   not ready: {msg}")
        time.sleep(5)
    raise TimeoutError(f"Bronze Delta table not found for {label}: {path}")


def ensure_silver_tables(topics: list[str]) -> None:
    for topic in topics:
        path = f"{HDFS_SILVER_BASE}/{topic}"
        try:
            spark.read.format("delta").load(path).schema
            continue
        except Exception:
            pass

        (
            spark.createDataFrame([], SILVER_SCHEMAS[topic])
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("event_date")
            .save(path)
        )


def base_columns(df: DataFrame) -> DataFrame:
    return df.select(
        col("user_id"),
        col("kafka_ingest_time"),
        col("event_date").cast(DateType()).alias("event_date"),
        col("event_hour").cast(FloatType()).cast(IntegerType()).alias("event_hour"),
        col("event_timestamp").cast(TimestampType()).alias("event_timestamp"),
        col("source_timestamp").cast(TimestampType()).alias("source_timestamp"),
        col("trace_id"),
        col("event_type"),
        col("payload"),
    )


def clean_stream(topic: str) -> DataFrame:
    bronze_path = f"{HDFS_BRONZE_BASE}/{topic}"
    wait_for_delta_table(bronze_path, topic)

    raw = spark.readStream.format("delta").load(bronze_path)
    parsed = base_columns(raw).withColumn("p", from_json(col("payload"), SCHEMAS[topic]))

    if topic == "heart_rate_intraday":
        return (
            parsed.select(
                "user_id",
                "kafka_ingest_time",
                "event_date",
                "event_hour",
                "event_timestamp",
                "source_timestamp",
                "trace_id",
                "event_type",
                col("p.bpm").alias("bpm"),
                current_timestamp().alias("processed_at"),
            )
            .dropna(subset=["user_id", "event_timestamp"])
            .filter(col("bpm").isNull() | col("bpm").between(30.0, 220.0))
        )

    if topic == "hrv_intraday":
        return (
            parsed.select(
                "user_id",
                "kafka_ingest_time",
                "event_date",
                "event_hour",
                "event_timestamp",
                "source_timestamp",
                "trace_id",
                "event_type",
                col("p.rmssd").alias("rmssd"),
                current_timestamp().alias("processed_at"),
            )
            .dropna(subset=["user_id", "event_timestamp"])
            .filter(col("rmssd").isNull() | col("rmssd").between(0.0, 300.0))
        )

    return (
        parsed.select(
            "user_id",
            "kafka_ingest_time",
            "event_date",
            "event_hour",
            "event_timestamp",
            "source_timestamp",
            "trace_id",
            "event_type",
            col("p.breaths_per_minute").alias("breaths_per_minute"),
            current_timestamp().alias("processed_at"),
        )
        .dropna(subset=["user_id", "event_timestamp"])
        .filter(col("breaths_per_minute").isNull() | col("breaths_per_minute").between(5.0, 40.0))
    )


def output_columns(topic: str) -> list[str]:
    common = [
        "user_id",
        "kafka_ingest_time",
        "event_date",
        "event_hour",
        "event_timestamp",
        "event_type",
    ]
    metric = {
        "heart_rate_intraday": "bpm",
        "hrv_intraday": "rmssd",
        "breathing_intraday": "breaths_per_minute",
    }[topic]
    return [*common, metric, "processed_at"]


def start_query(topic: str):
    df = clean_stream(topic).select(*output_columns(topic))
    if SILVER_WRITE_COALESCE > 0:
        df = df.coalesce(SILVER_WRITE_COALESCE)
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{topic}")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .partitionBy("event_date")
        .start(f"{HDFS_SILVER_BASE}/{topic}")
    )


if __name__ == "__main__":
    selected = [t for t in SCHEMAS if t in SILVER_ONLY]
    if not selected:
        raise ValueError(f"No supported intraday Silver tables selected: {sorted(SILVER_ONLY)}")

    print(f"[silver-stream] Starting tables: {', '.join(selected)}")
    ensure_silver_tables(selected)
    queries = [start_query(topic) for topic in selected]
    print("[silver-stream] Structured Streaming started:")
    for topic in selected:
        print(f"[silver-stream]   {topic}")
    spark.streams.awaitAnyTermination()
