"""
GOLD LAYER - Spark Structured Streaming: Silver Delta -> Gold Delta.

This job watches the three intraday Silver Delta tables and incrementally
refreshes daily_intraday_summary. Each micro-batch identifies the affected
(user_id, event_date) keys, recomputes only those keys from Silver, and upserts
them into the Gold Delta table.

Note: _refresh_lock was removed. Spark Structured Streaming with foreachBatch
guarantees sequential micro-batch execution — batch N+1 never starts until
batch N's foreachBatch call returns. A non-blocking lock therefore has no
protective effect and causes Silver micro-batches to be permanently skipped
(Spark advances the checkpoint even when foreachBatch returns early), resulting
in an incomplete Gold table.
"""

from __future__ import annotations

import os
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, current_timestamp, lit, max as spark_max, min as spark_min, stddev
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType


HDFS_SILVER_BASE = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
HDFS_GOLD_BASE = os.getenv("HDFS_GOLD_BASE", "hdfs://namenode:9000/data/gold/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/gold/wearable")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "18")
TRIGGER_SECONDS = os.getenv("GOLD_TRIGGER_SECONDS", "30")
STARTUP_WAIT_SECONDS = int(os.getenv("STARTUP_WAIT_SECONDS", "300"))
GOLD_MERGE_RETRIES = int(os.getenv("GOLD_MERGE_RETRIES", "5"))
GOLD_MERGE_RETRY_SLEEP_SECONDS = float(os.getenv("GOLD_MERGE_RETRY_SLEEP_SECONDS", "5"))

TABLES = ("heart_rate_intraday", "hrv_intraday", "breathing_intraday")
USER_DAY_KEYS = ["user_id", "event_date"]
GOLD_TABLE = f"{HDFS_GOLD_BASE}/daily_intraday_summary"

GOLD_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("event_date", DateType()),
    StructField("intraday_avg_bpm", DoubleType()),
    StructField("intraday_min_bpm", DoubleType()),
    StructField("intraday_max_bpm", DoubleType()),
    StructField("intraday_stddev_bpm", DoubleType()),
    StructField("intraday_hr_readings", LongType()),
    StructField("intraday_avg_rmssd", DoubleType()),
    StructField("intraday_min_rmssd", DoubleType()),
    StructField("intraday_max_rmssd", DoubleType()),
    StructField("intraday_avg_breathing", DoubleType()),
    StructField("intraday_min_breathing", DoubleType()),
    StructField("intraday_max_breathing", DoubleType()),
    StructField("gold_updated_at", TimestampType()),
])


spark = (
    SparkSession.builder.appName("WearableGoldStreamingIntraday")
    .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def wait_for_delta_table(path: str, label: str) -> None:
    start = time.time()
    while time.time() - start < STARTUP_WAIT_SECONDS:
        try:
            spark.read.format("delta").load(path).schema
            return
        except Exception as exc:
            msg = str(exc).splitlines()[0]
        print(f"[gold-stream] Waiting for Silver/{label} at {path} ...")
        if msg:
            print(f"[gold-stream]   not ready: {msg}")
        time.sleep(5)
    raise TimeoutError(f"Silver Delta table not found for {label}: {path}")


def read_silver(topic: str) -> DataFrame:
    return spark.read.format("delta").load(f"{HDFS_SILVER_BASE}/{topic}")


def ensure_gold_table() -> None:
    if DeltaTable.isDeltaTable(spark, GOLD_TABLE):
        return
    (
        spark.createDataFrame([], GOLD_SCHEMA)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(GOLD_TABLE)
    )


def read_silver_for_keys(topic: str, keys: DataFrame) -> DataFrame:
    return read_silver(topic).join(broadcast(keys), on=USER_DAY_KEYS, how="inner")


def build_trigger_stream() -> DataFrame:
    streams = []
    for topic in TABLES:
        path = f"{HDFS_SILVER_BASE}/{topic}"
        wait_for_delta_table(path, topic)
        stream = (
            spark.readStream.format("delta")
            .load(path)
            .select("user_id", "event_date", "event_timestamp")
            .withColumn("source_table", lit(topic))
        )
        streams.append(stream)

    trigger = streams[0]
    for stream in streams[1:]:
        trigger = trigger.unionByName(stream)
    return trigger


def refresh_gold(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    try:

        keys = (
            batch_df.select(*USER_DAY_KEYS)
            .dropna(subset=USER_DAY_KEYS)
            .distinct()
            .cache()
        )
        key_count = keys.count()
        if key_count == 0:
            return

        hr = read_silver_for_keys("heart_rate_intraday", keys)
        hrv = read_silver_for_keys("hrv_intraday", keys)
        br = read_silver_for_keys("breathing_intraday", keys)

        hr_daily = hr.groupBy(*USER_DAY_KEYS).agg(
            avg("bpm").alias("intraday_avg_bpm"),
            spark_min("bpm").alias("intraday_min_bpm"),
            spark_max("bpm").alias("intraday_max_bpm"),
            stddev("bpm").alias("intraday_stddev_bpm"),
            count("bpm").alias("intraday_hr_readings"),
        )
        hrv_daily = hrv.groupBy(*USER_DAY_KEYS).agg(
            avg("rmssd").alias("intraday_avg_rmssd"),
            spark_min("rmssd").alias("intraday_min_rmssd"),
            spark_max("rmssd").alias("intraday_max_rmssd"),
        )
        br_daily = br.groupBy(*USER_DAY_KEYS).agg(
            avg("breaths_per_minute").alias("intraday_avg_breathing"),
            spark_min("breaths_per_minute").alias("intraday_min_breathing"),
            spark_max("breaths_per_minute").alias("intraday_max_breathing"),
        )

        gold = (
            keys.join(hr_daily, on=USER_DAY_KEYS, how="left")
            .join(hrv_daily, on=USER_DAY_KEYS, how="left")
            .join(br_daily, on=USER_DAY_KEYS, how="left")
            .select(
                col("user_id"),
                col("event_date"),
                col("intraday_avg_bpm").cast(DoubleType()).alias("intraday_avg_bpm"),
                col("intraday_min_bpm").cast(DoubleType()).alias("intraday_min_bpm"),
                col("intraday_max_bpm").cast(DoubleType()).alias("intraday_max_bpm"),
                col("intraday_stddev_bpm").cast(DoubleType()).alias("intraday_stddev_bpm"),
                col("intraday_hr_readings").cast(LongType()).alias("intraday_hr_readings"),
                col("intraday_avg_rmssd").cast(DoubleType()).alias("intraday_avg_rmssd"),
                col("intraday_min_rmssd").cast(DoubleType()).alias("intraday_min_rmssd"),
                col("intraday_max_rmssd").cast(DoubleType()).alias("intraday_max_rmssd"),
                col("intraday_avg_breathing").cast(DoubleType()).alias("intraday_avg_breathing"),
                col("intraday_min_breathing").cast(DoubleType()).alias("intraday_min_breathing"),
                col("intraday_max_breathing").cast(DoubleType()).alias("intraday_max_breathing"),
            )
            .withColumn("gold_updated_at", current_timestamp())
        )

        ensure_gold_table()
        for attempt in range(1, GOLD_MERGE_RETRIES + 1):
            try:
                target = DeltaTable.forPath(spark, GOLD_TABLE)
                (
                    target.alias("t")
                    .merge(
                        gold.alias("s"),
                        "t.user_id = s.user_id AND t.event_date = s.event_date",
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                break
            except Exception as exc:
                msg = str(exc)
                is_metadata_conflict = (
                    "MetadataChangedException" in msg
                    or "DELTA_METADATA_CHANGED" in msg
                    or "Concurrent" in msg
                )
                if not is_metadata_conflict or attempt >= GOLD_MERGE_RETRIES:
                    raise
                print(
                    f"[gold-stream] Batch {batch_id} merge hit Delta metadata conflict; "
                    f"retrying {attempt}/{GOLD_MERGE_RETRIES} in {GOLD_MERGE_RETRY_SLEEP_SECONDS}s."
                )
                time.sleep(GOLD_MERGE_RETRY_SLEEP_SECONDS)
        print(f"[gold-stream] Upserted {key_count} daily_intraday_summary key(s) from batch {batch_id}")
    finally:
        try:
            keys.unpersist()
        except Exception:
            pass


if __name__ == "__main__":
    trigger = build_trigger_stream()
    query = (
        trigger.writeStream.foreachBatch(refresh_gold)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/daily_intraday_summary")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    print("[gold-stream] Structured Streaming started. Waiting for data ...")
    query.awaitTermination()
