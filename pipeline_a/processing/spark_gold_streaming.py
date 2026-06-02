"""
GOLD LAYER - Spark Structured Streaming: Silver Delta -> Gold Delta.

This job maintains daily_intraday_summary as a STATEFUL, incremental streaming
aggregation. Spark Structured Streaming keeps a running aggregate per
(user_id, event_date) in its state store; each micro-batch processes only the
new Silver rows that arrived since the last trigger, updates the in-state
aggregate, and upserts the changed keys into the Gold Delta table.

WHY THIS DESIGN (fairness note):
  The previous version was a *stateless full re-aggregation*: every micro-batch
  re-read the entire Silver table for the touched keys (a broadcast join over
  the full Delta table) and recomputed each aggregate from scratch. That made
  Gold cost grow with the accumulated Silver size and made the Spark/Delta
  pipeline far slower than necessary. Flink's Gold job (pipeline_b/processing/
  flink_gold.py) uses a continuous, stateful GROUP BY maintained in managed
  state. To compare the two stacks on an equal algorithmic footing, this Spark
  Gold now mirrors that approach: a streaming GROUP BY over a UNION of the three
  intraday Silver streams, in update output mode, with an idempotent Delta MERGE
  upsert per trigger. Like flink_gold.py it uses NO watermark, so state is kept
  for all keys (the (user_id, event_date) key set is bounded for this workload).

Note: foreachBatch guarantees sequential micro-batch execution -- batch N+1
never starts until batch N's callback returns -- so the MERGE upsert is safe
without an external lock.
"""

from __future__ import annotations

import os
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, lit, max as spark_max, min as spark_min, stddev
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType


HDFS_SILVER_BASE = os.getenv("HDFS_SILVER_BASE", "hdfs://namenode:9000/data/silver/wearable")
HDFS_GOLD_BASE = os.getenv("HDFS_GOLD_BASE", "hdfs://namenode:9000/data/gold/wearable")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "hdfs://namenode:9000/checkpoints/gold/wearable")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "18")
TRIGGER_SECONDS = os.getenv("GOLD_TRIGGER_SECONDS", "15")
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


def _silver_readstream(topic: str) -> DataFrame:
    """Incremental streaming read of one Silver Delta table.

    Spark processes the existing snapshot as the first micro-batch and then
    streams only newly committed files on each subsequent trigger -- it never
    re-reads the whole table. This is the incremental ingest that replaces the
    old full-table re-scan.
    """
    path = f"{HDFS_SILVER_BASE}/{topic}"
    wait_for_delta_table(path, topic)
    return spark.readStream.format("delta").load(path)


def build_gold_stream() -> DataFrame:
    """Union the three intraday Silver streams and aggregate per (user, day).

    Each source contributes one signal column; the other two are NULL, so the
    AVG/MIN/MAX/COUNT/STDDEV aggregates (which ignore NULLs) compute exactly the
    same daily summary as before -- but now incrementally and statefully.
    """
    hr = _silver_readstream("heart_rate_intraday").select(
        col("user_id"),
        col("event_date"),
        col("bpm").cast(DoubleType()).alias("bpm"),
        lit(None).cast(DoubleType()).alias("rmssd"),
        lit(None).cast(DoubleType()).alias("breaths_per_minute"),
    )
    hrv = _silver_readstream("hrv_intraday").select(
        col("user_id"),
        col("event_date"),
        lit(None).cast(DoubleType()).alias("bpm"),
        col("rmssd").cast(DoubleType()).alias("rmssd"),
        lit(None).cast(DoubleType()).alias("breaths_per_minute"),
    )
    br = _silver_readstream("breathing_intraday").select(
        col("user_id"),
        col("event_date"),
        lit(None).cast(DoubleType()).alias("bpm"),
        lit(None).cast(DoubleType()).alias("rmssd"),
        col("breaths_per_minute").cast(DoubleType()).alias("breaths_per_minute"),
    )

    combined = (
        hr.unionByName(hrv)
        .unionByName(br)
        .filter(col("user_id").isNotNull() & col("event_date").isNotNull())
    )

    # Stateful streaming aggregation. Spark keeps the running aggregate for each
    # (user_id, event_date) in its state store and updates it from new rows
    # only. No watermark -> unbounded state, matching flink_gold.py.
    return combined.groupBy("user_id", "event_date").agg(
        avg("bpm").cast(DoubleType()).alias("intraday_avg_bpm"),
        spark_min("bpm").cast(DoubleType()).alias("intraday_min_bpm"),
        spark_max("bpm").cast(DoubleType()).alias("intraday_max_bpm"),
        stddev("bpm").cast(DoubleType()).alias("intraday_stddev_bpm"),
        count("bpm").cast(LongType()).alias("intraday_hr_readings"),
        avg("rmssd").cast(DoubleType()).alias("intraday_avg_rmssd"),
        spark_min("rmssd").cast(DoubleType()).alias("intraday_min_rmssd"),
        spark_max("rmssd").cast(DoubleType()).alias("intraday_max_rmssd"),
        avg("breaths_per_minute").cast(DoubleType()).alias("intraday_avg_breathing"),
        spark_min("breaths_per_minute").cast(DoubleType()).alias("intraday_min_breathing"),
        spark_max("breaths_per_minute").cast(DoubleType()).alias("intraday_max_breathing"),
    )


def upsert_gold(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert the keys that changed in this micro-batch into the Gold table.

    In update output mode, batch_df already holds the CURRENT aggregate for each
    key whose value changed this trigger (computed from state, not from a Silver
    re-scan), so we only MERGE those rows.
    """
    batch_df = batch_df.persist()
    try:
        changed = batch_df.count()
        if changed == 0:
            return

        gold = batch_df.withColumn("gold_updated_at", current_timestamp())

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
        print(f"[gold-stream] Upserted {changed} changed key(s) from batch {batch_id}")
    finally:
        try:
            batch_df.unpersist()
        except Exception:
            pass


if __name__ == "__main__":
    gold_stream = build_gold_stream()
    query = (
        gold_stream.writeStream
        .outputMode("update")
        .foreachBatch(upsert_gold)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/daily_intraday_summary")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    print("[gold-stream] Stateful streaming aggregation started. Waiting for data ...")
    query.awaitTermination()
