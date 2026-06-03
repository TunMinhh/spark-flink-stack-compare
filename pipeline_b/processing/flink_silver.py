"""
Silver layer: Bronze Iceberg → Silver Iceberg (TRUE STREAMING)

This continuously running Flink streaming job defaults to the realtime intraday
path only: heart_rate_intraday, hrv_intraday, and breathing_intraday. It reads
Bronze Iceberg tables in incremental streaming mode, parses + cleans + filters
records, and writes them to Silver Iceberg tables via append-only INSERT INTO.

Unlike the batch version, there is no dedup ROW_NUMBER pass: exactly-once
delivery from Bronze (Flink checkpointing) guarantees Silver appends each
record at most once. The selected tables are bundled into a single StatementSet
so they execute as one Flink job (one TaskManager graph, shared resources).

Submit via:  `make silver`  → calls `flink run -py flink_silver.py …`
The script returns immediately after job submission; the job runs forever
inside the Flink cluster until explicitly cancelled.
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

ICEBERG_URI = os.environ["ICEBERG_CATALOG_URI"]
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")
PARALLELISM = int(os.environ.get("PARALLELISM", "6"))

# Iceberg streaming source poll interval — lower = lower latency, more S3 LIST calls.
# 10 s is a reasonable balance for benchmark; production might use 30 s.
MONITOR_INTERVAL    = os.environ.get("ICEBERG_MONITOR_INTERVAL",     "15s")
CHECKPOINT_INTERVAL = os.environ.get("FLINK_CHECKPOINT_INTERVAL",    "15 s")
CHECKPOINT_MIN_PAUSE = os.environ.get("FLINK_CHECKPOINT_MIN_PAUSE",  "5 s")
REALTIME_TABLES = {
    "heart_rate_intraday",
    "hrv_intraday",
    "breathing_intraday",
}
REQUESTED_SILVER = {
    name.strip()
    for name in os.environ.get("SILVER_ONLY", "").split(",")
    if name.strip()
}
IGNORED_SILVER = REQUESTED_SILVER - REALTIME_TABLES
SILVER_SELECTED = (REQUESTED_SILVER & REALTIME_TABLES) or REALTIME_TABLES

t_env = TableEnvironment.create(
    EnvironmentSettings.new_instance().in_streaming_mode().build()
)
t_env.get_config().set("parallelism.default", str(PARALLELISM))

cfg = t_env.get_config().get_configuration()

# ── Checkpointing (required for exactly-once Iceberg writes) ──────────────────
cfg.set_string("execution.checkpointing.interval",        CHECKPOINT_INTERVAL)
cfg.set_string("execution.checkpointing.mode",            "EXACTLY_ONCE")
cfg.set_string("execution.checkpointing.timeout",         "10 min")
cfg.set_string("execution.checkpointing.min-pause",       CHECKPOINT_MIN_PAUSE)
cfg.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
cfg.set_string("state.checkpoints.dir",                   os.environ.get("FLINK_CHECKPOINT_DIR_SILVER", "hdfs://namenode:9000/checkpoints/flink/silver"))
cfg.set_string("restart-strategy.type",                   "fixed-delay")
cfg.set_string("restart-strategy.fixed-delay.attempts",   "3")
cfg.set_string("restart-strategy.fixed-delay.delay",      "10 s")

# ── S3 / MinIO ────────────────────────────────────────────────────────────────
cfg.set_string("fs.default-scheme", "hdfs://namenode:9000")
cfg.set_string("fs.hdfs.hadoopconf", "/opt/hadoop/etc/hadoop")

# ── Iceberg catalog ───────────────────────────────────────────────────────────
if "iceberg_cat" not in t_env.list_catalogs():
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg_cat WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'rest',
            'uri'                  = '{ICEBERG_URI}',
            'io-impl'              = 'org.apache.iceberg.hadoop.HadoopFileIO',
            'warehouse'            = '{ICEBERG_WAREHOUSE}'
        )
    """)
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_cat.silver")


# ---------------------------------------------------------------------------
# Statement set — bundles the realtime intraday INSERTs into ONE Flink job.
# Without this, each execute_sql("INSERT …") would launch a separate job and
# blow up resource usage. With it, Flink builds a single dataflow graph with
# 3 source/sink pairs sharing the same TaskManager slots.
# ---------------------------------------------------------------------------
stmt_set = t_env.create_statement_set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enabled(table_name: str) -> bool:
    return table_name in SILVER_SELECTED


def _create(ddl: str) -> None:
    """Execute DDL synchronously (CREATE TABLE / DATABASE are not streaming ops)."""
    t_env.execute_sql(ddl).wait()


def _src(bronze_table: str) -> str:
    """
    Build the FROM clause for a streaming Iceberg source.
    Uses dynamic table options to enable incremental streaming reads.
    """
    return (
        f"iceberg_cat.bronze.{bronze_table} "
        f"/*+ OPTIONS('streaming'='true', "
        f"'monitor-interval'='{MONITOR_INTERVAL}', "
        f"'starting-strategy'='INCREMENTAL_FROM_EARLIEST_SNAPSHOT') */"
    )


# Envelope columns with correct Spark-matching casts, shared by every topic.
_ENVELOPE = """
            user_id,
            kafka_ingest_time,
            TO_DATE(SUBSTRING(event_date, 1, 10))                  AS event_date,
            CAST(CAST(event_hour AS DOUBLE) AS INT)               AS event_hour,
            TO_TIMESTAMP(REPLACE(event_timestamp, 'T', ' '))      AS event_timestamp,
            event_type"""

_BASE_WHERE = "user_id IS NOT NULL AND event_timestamp IS NOT NULL"


# ---------------------------------------------------------------------------
# heart_rate_intraday  (high-volume per-second)
# ---------------------------------------------------------------------------
def process_heart_rate_intraday() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.heart_rate_intraday (
            user_id           STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date        DATE,
            event_hour        INT,
            event_timestamp   TIMESTAMP(6),
            event_type        STRING,
            bpm               FLOAT,
            processed_at      TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.heart_rate_intraday
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.bpm') AS FLOAT) AS bpm,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('heart_rate_intraday')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.bpm') IS NULL
               OR CAST(JSON_VALUE(payload, '$.bpm') AS FLOAT) BETWEEN 30.0 AND 220.0)
    """)


# ---------------------------------------------------------------------------
# hrv_intraday
# ---------------------------------------------------------------------------
def process_hrv_intraday() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.hrv_intraday (
            user_id           STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date        DATE,
            event_hour        INT,
            event_timestamp   TIMESTAMP(6),
            event_type        STRING,
            rmssd             FLOAT,
            processed_at      TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.hrv_intraday
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.rmssd') AS FLOAT) AS rmssd,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('hrv_intraday')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.rmssd') IS NULL
               OR CAST(JSON_VALUE(payload, '$.rmssd') AS FLOAT) BETWEEN 0.0 AND 300.0)
    """)


# ---------------------------------------------------------------------------
# breathing_intraday
# ---------------------------------------------------------------------------
def process_breathing_intraday() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.breathing_intraday (
            user_id            STRING,
            kafka_ingest_time  TIMESTAMP(6),
            event_date         DATE,
            event_hour         INT,
            event_timestamp    TIMESTAMP(6),
            event_type         STRING,
            breaths_per_minute FLOAT,
            processed_at       TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.breathing_intraday
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.breaths_per_minute') AS FLOAT) AS breaths_per_minute,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('breathing_intraday')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.breaths_per_minute') IS NULL
               OR CAST(JSON_VALUE(payload, '$.breaths_per_minute') AS FLOAT) BETWEEN 5.0 AND 40.0)
    """)


# ---------------------------------------------------------------------------
# Main — register sinks, build statement set, submit ONE streaming job.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if IGNORED_SILVER:
        print(f"[silver] Ignoring non-realtime table(s): {', '.join(sorted(IGNORED_SILVER))}")
    print(f"[silver] Selected realtime pipeline(s): {', '.join(sorted(SILVER_SELECTED))}")

    if _enabled("heart_rate_intraday"):
        process_heart_rate_intraday()
    if _enabled("hrv_intraday"):
        process_hrv_intraday()
    if _enabled("breathing_intraday"):
        process_breathing_intraday()

    print("[silver] Submitting streaming job (sources to sinks, single job graph) …")
    job_client = stmt_set.execute()
    # In cluster mode (flink run), this returns immediately. In local mode the
    # client would block; we don't wait() so behaviour is consistent.
    print("[silver] Job submitted. It runs forever until cancelled.")
    print("[silver] Monitor via Flink UI at http://localhost:8082/")
