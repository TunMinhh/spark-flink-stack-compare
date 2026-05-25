"""
BRONZE LAYER — Spark Structured Streaming ingest from Kafka to HDFS (Delta Lake).
See processing/README.md for full documentation.
"""

import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
HDFS_BRONZE_BASE = os.getenv("HDFS_BRONZE_BASE", "hdfs://namenode:9000/data/bronze/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/bronze/wearable")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")

DEFAULT_TOPICS = (
    "wearable_heart_rate_intraday,"
    "wearable_hrv_intraday,"
    "wearable_breathing_intraday"
)
TOPICS = os.getenv("BRONZE_TOPICS", DEFAULT_TOPICS)
# Default 4 suits a single-node local setup. On a VPS set SHUFFLE_PARTITIONS
# to 2× the total number of executor cores across all workers.
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "18")
TRIGGER_SECONDS = os.getenv("BRONZE_TRIGGER_SECONDS", "10")

# ── Session ───────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("WearableBronzeIngest")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── [Bronze] Write raw partitioned Parquet to HDFS Bronze zone ───────────────
TOPIC_NAMES = {
    "wearable_heart_rate_intraday": "heart_rate_intraday",
    "wearable_hrv_intraday":        "hrv_intraday",
    "wearable_breathing_intraday":  "breathing_intraday",
}
SELECTED_TOPICS = {topic.strip() for topic in TOPICS.split(",") if topic.strip()}
SELECTED_TOPIC_NAMES = {
    topic: folder
    for topic, folder in TOPIC_NAMES.items()
    if topic in SELECTED_TOPICS
}


BRONZE_TABLE_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("kafka_ingest_time", TimestampType()),
    StructField("event_date", StringType()),
    StructField("event_hour", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("source_timestamp", StringType()),
    StructField("trace_id", StringType()),
    StructField("event_type", StringType()),
    StructField("payload", StringType()),
])

ENVELOPE_SCHEMA = StructType([
    StructField("event_date", StringType()),
    StructField("event_hour", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("source_timestamp", StringType()),
    StructField("trace_id", StringType()),
    StructField("event_type", StringType()),
])


def ensure_empty_delta_tables() -> None:
    empty_df = spark.createDataFrame([], BRONZE_TABLE_SCHEMA)
    for folder in SELECTED_TOPIC_NAMES.values():
        path = f"{HDFS_BRONZE_BASE}/{folder}"
        try:
            spark.read.format("delta").load(path).schema
            continue
        except Exception:
            mode = "overwrite"
        (
            empty_df.write.format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .partitionBy("event_date")
            .save(path)
        )


def read_topic_stream(topic: str) -> DataFrame:
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_df = raw_df.select(
        col("key").cast(StringType()).alias("user_id"),
        col("value").cast(StringType()).alias("raw_json"),
        col("timestamp").alias("kafka_ingest_time"),
    ).withColumn("e", from_json(col("raw_json"), ENVELOPE_SCHEMA))
    return parsed_df.select(
        col("user_id"),
        col("kafka_ingest_time"),
        col("e.event_date").alias("event_date"),
        col("e.event_hour").alias("event_hour"),
        col("e.event_timestamp").alias("event_timestamp"),
        col("e.source_timestamp").alias("source_timestamp"),
        col("e.trace_id").alias("trace_id"),
        col("e.event_type").alias("event_type"),
        # payload is kept as a raw JSON string — Silver layer is responsible for parsing it
        get_json_object(col("raw_json"), "$.payload").alias("payload"),
    )


def start_topic_query(topic: str, folder: str):
    return (
        read_topic_stream(topic)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{folder}")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .partitionBy("event_date")
        .start(f"{HDFS_BRONZE_BASE}/{folder}")
    )


ensure_empty_delta_tables()

queries = [
    start_topic_query(topic, folder)
    for topic, folder in sorted(SELECTED_TOPIC_NAMES.items())
]

print("[bronze] Streaming started:")
for topic, folder in sorted(SELECTED_TOPIC_NAMES.items()):
    print(f"[bronze]   {topic} -> {folder}")

spark.streams.awaitAnyTermination()
