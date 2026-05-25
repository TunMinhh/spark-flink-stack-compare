"""
Bronze layer: Kafka → Iceberg (streaming, continuous)

All realtime intraday topics share the same Bronze schema — the Kafka message key carries
user_id and the value is a JSON envelope identical to what Spark Bronze reads:

  { event_date, event_hour, event_timestamp, source_timestamp,
    trace_id, event_type, payload: {...} }

`payload` is kept as a raw JSON string; Silver is responsible for parsing it.
This mirrors Spark's foreachBatch envelope approach exactly.

Run via:  make bronze   (runs continuously — Ctrl-C to stop)
"""

import os

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BOOTSTRAP  = os.environ["KAFKA_BOOTSTRAP"]
ICEBERG_URI      = os.environ["ICEBERG_CATALOG_URI"]
S3_ENDPOINT      = os.environ["S3_ENDPOINT"]
AWS_KEY          = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET       = os.environ["AWS_SECRET_ACCESS_KEY"]
PARALLELISM      = int(os.environ.get("PARALLELISM", "6"))
CHECKPOINT_INTERVAL_MS = int(float(os.environ.get("FLINK_CHECKPOINT_INTERVAL_SECONDS", "15")) * 1000)
CHECKPOINT_DIR   = "s3://mlflow/flink-checkpoints/bronze"
BRONZE_ONLY = {
    name.strip()
    for name in os.environ.get("BRONZE_ONLY", "").split(",")
    if name.strip()
}

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(PARALLELISM)
env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_DIR)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, settings)

cfg = t_env.get_config().get_configuration()
cfg.set_string("s3.endpoint", S3_ENDPOINT)
cfg.set_string("s3.path-style-access", "true")
cfg.set_string("s3.access-key", AWS_KEY)
cfg.set_string("s3.secret-key", AWS_SECRET)

if "iceberg_cat" not in t_env.list_catalogs():
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg_cat WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'rest',
            'uri'                  = '{ICEBERG_URI}',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = '{S3_ENDPOINT}',
            's3.path-style-access' = 'true',
            'warehouse'            = 's3://iceberg/'
        )
    """)
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_cat.bronze")

# ---------------------------------------------------------------------------
# The realtime-only pipeline ingests just the three producer_realtime topics.
# Other topic mappings remain below for explicit experiments via BRONZE_ONLY.
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = """(
    user_id           STRING,
    kafka_ingest_time TIMESTAMP(6),
    event_date        STRING,
    event_hour        STRING,
    event_timestamp   STRING,
    source_timestamp  STRING,
    trace_id          STRING,
    event_type        STRING,
    payload           STRING
) PARTITIONED BY (event_date)
WITH ('format-version' = '2')"""

TOPICS = [
    "heart_rate_intraday",
    "hrv_intraday",
    "breathing_intraday",
]
if BRONZE_ONLY:
    TOPICS = [topic for topic in TOPICS if topic in BRONZE_ONLY]

KAFKA_TOPIC_MAP = {
    "heart_rate_intraday":  "wearable_heart_rate_intraday",
    "hrv_intraday":         "wearable_hrv_intraday",
    "breathing_intraday":   "wearable_breathing_intraday",
}

stmt = t_env.create_statement_set()

for folder in TOPICS:
    kafka_topic = KAFKA_TOPIC_MAP[folder]
    raw_src = f"kafka_{folder}_raw"
    view    = f"kafka_{folder}"
    sink    = f"iceberg_cat.bronze.{folder}"

    # ── Step 1: raw Kafka source (key = user_id, value = raw JSON string) ────
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE {raw_src} (
            user_id           STRING,
            raw_value         STRING,
            kafka_ingest_time TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR kafka_ingest_time AS kafka_ingest_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                            = 'kafka',
            'topic'                                = '{kafka_topic}',
            'properties.bootstrap.servers'         = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'                  = 'flink-bronze-{folder}',
            'scan.startup.mode'                    = 'earliest-offset',
            'key.format'                           = 'raw',
            'key.fields'                           = 'user_id',
            'value.format'                         = 'raw',
            'value.fields-include'                 = 'EXCEPT_KEY'
        )
    """)

    # ── Step 2: view that extracts envelope fields via JSON functions ─────────
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW {view} AS
        SELECT
            user_id,
            CAST(kafka_ingest_time AS TIMESTAMP(6)) AS kafka_ingest_time,
            JSON_VALUE(raw_value, '$.event_date')        AS event_date,
            JSON_VALUE(raw_value, '$.event_hour')        AS event_hour,
            JSON_VALUE(raw_value, '$.event_timestamp')   AS event_timestamp,
            JSON_VALUE(raw_value, '$.source_timestamp')  AS source_timestamp,
            JSON_VALUE(raw_value, '$.trace_id')          AS trace_id,
            JSON_VALUE(raw_value, '$.event_type')        AS event_type,
            JSON_QUERY(raw_value, '$.payload')           AS payload
        FROM {raw_src}
    """)

    # ── Step 3: Bronze Iceberg sink ───────────────────────────────────────────
    t_env.execute_sql(f"CREATE TABLE IF NOT EXISTS {sink} {BRONZE_SCHEMA}")

    # ── Step 4: queue the insert ──────────────────────────────────────────────
    stmt.add_insert(sink, t_env.from_path(view))

stmt.execute()
print("[bronze] Job submitted. It runs forever until cancelled.")
print("[bronze] Monitor via Flink UI at http://localhost:8082/")
