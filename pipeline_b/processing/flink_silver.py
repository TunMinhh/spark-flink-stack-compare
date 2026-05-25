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
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
AWS_KEY     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET  = os.environ["AWS_SECRET_ACCESS_KEY"]
PARALLELISM = int(os.environ.get("PARALLELISM", "6"))

# Iceberg streaming source poll interval — lower = lower latency, more S3 LIST calls.
# 10 s is a reasonable balance for benchmark; production might use 30 s.
MONITOR_INTERVAL = os.environ.get("ICEBERG_MONITOR_INTERVAL", "15s")
CHECKPOINT_INTERVAL = os.environ.get("FLINK_CHECKPOINT_INTERVAL", "15 s")
SILVER_ONLY = {
    name.strip()
    for name in os.environ.get("SILVER_ONLY", "").split(",")
    if name.strip()
}

t_env = TableEnvironment.create(
    EnvironmentSettings.new_instance().in_streaming_mode().build()
)
t_env.get_config().set("parallelism.default", str(PARALLELISM))

cfg = t_env.get_config().get_configuration()

# ── Checkpointing (required for exactly-once Iceberg writes) ──────────────────
cfg.set_string("execution.checkpointing.interval",        CHECKPOINT_INTERVAL)
cfg.set_string("execution.checkpointing.mode",            "EXACTLY_ONCE")
cfg.set_string("execution.checkpointing.timeout",         "10 min")
cfg.set_string("execution.checkpointing.min-pause",       "5 s")
cfg.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
cfg.set_string("state.checkpoints.dir",                   "s3://mlflow/flink-checkpoints/silver")
cfg.set_string("restart-strategy.type",                   "fixed-delay")
cfg.set_string("restart-strategy.fixed-delay.attempts",   "3")
cfg.set_string("restart-strategy.fixed-delay.delay",      "10 s")

# ── S3 / MinIO ────────────────────────────────────────────────────────────────
cfg.set_string("s3.endpoint",          S3_ENDPOINT)
cfg.set_string("s3.path-style-access", "true")
cfg.set_string("s3.access-key",        AWS_KEY)
cfg.set_string("s3.secret-key",        AWS_SECRET)

# ── Iceberg catalog ───────────────────────────────────────────────────────────
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
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_cat.silver")


# ---------------------------------------------------------------------------
# Statement set — bundles all 11 INSERTs into ONE Flink job.
# Without this, each execute_sql("INSERT …") would launch a separate job and
# blow up resource usage. With it, Flink builds a single dataflow graph with
# 11 source/sink pairs sharing the same TaskManager slots.
# ---------------------------------------------------------------------------
stmt_set = t_env.create_statement_set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _enabled(table_name: str) -> bool:
    return not SILVER_ONLY or table_name in SILVER_ONLY


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
# vitals
# ---------------------------------------------------------------------------
def process_vitals() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.vitals (
            user_id           STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date        DATE,
            event_hour        INT,
            event_timestamp   TIMESTAMP(6),
            event_type        STRING,
            bpm               FLOAT,
            temperature       FLOAT,
            scl_avg           FLOAT,
            processed_at      TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.vitals
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.bpm')         AS FLOAT) AS bpm,
            CAST(JSON_VALUE(payload, '$.temperature') AS FLOAT) AS temperature,
            CAST(JSON_VALUE(payload, '$.scl_avg')     AS FLOAT) AS scl_avg,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('vitals')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.bpm')     IS NULL
               OR CAST(JSON_VALUE(payload, '$.bpm')     AS FLOAT) BETWEEN 30.0  AND 220.0)
          AND (JSON_VALUE(payload, '$.scl_avg') IS NULL
               OR CAST(JSON_VALUE(payload, '$.scl_avg') AS FLOAT) BETWEEN 0.0   AND 30.0)
    """)


# ---------------------------------------------------------------------------
# activity
# ---------------------------------------------------------------------------
def process_activity() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.activity (
            user_id              STRING,
            kafka_ingest_time    TIMESTAMP(6),
            event_date           DATE,
            event_hour           INT,
            event_timestamp      TIMESTAMP(6),
            event_type           STRING,
            calories             FLOAT,
            distance             FLOAT,
            steps                INT,
            activity_type        STRING,
            minutes_zone_1       INT,
            minutes_zone_2       INT,
            minutes_zone_3       INT,
            minutes_below_zone_1 INT,
            processed_at         TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.activity
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.calories')                       AS FLOAT) AS calories,
            CAST(JSON_VALUE(payload, '$.distance')                       AS FLOAT) AS distance,
            CAST(CAST(JSON_VALUE(payload, '$.steps') AS FLOAT)           AS INT)   AS steps,
            JSON_VALUE(payload, '$.activityType')                                  AS activity_type,
            CAST(CAST(JSON_VALUE(payload, '$.minutes_in_default_zone_1')     AS FLOAT) AS INT) AS minutes_zone_1,
            CAST(CAST(JSON_VALUE(payload, '$.minutes_in_default_zone_2')     AS FLOAT) AS INT) AS minutes_zone_2,
            CAST(CAST(JSON_VALUE(payload, '$.minutes_in_default_zone_3')     AS FLOAT) AS INT) AS minutes_zone_3,
            CAST(CAST(JSON_VALUE(payload, '$.minutes_below_default_zone_1')  AS FLOAT) AS INT) AS minutes_below_zone_1,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('activity')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.calories') IS NULL
               OR CAST(JSON_VALUE(payload, '$.calories') AS FLOAT) >= 0)
          AND (JSON_VALUE(payload, '$.distance')  IS NULL
               OR CAST(JSON_VALUE(payload, '$.distance')  AS FLOAT) >= 0.0)
          AND (JSON_VALUE(payload, '$.steps')     IS NULL
               OR CAST(JSON_VALUE(payload, '$.steps')     AS FLOAT) BETWEEN 0 AND 30000)
    """)


# ---------------------------------------------------------------------------
# context  — mood/location as individual BOOLEAN columns (matches Spark Silver)
# ---------------------------------------------------------------------------
def process_context() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.context (
            user_id             STRING,
            kafka_ingest_time   TIMESTAMP(6),
            event_date          DATE,
            event_hour          INT,
            event_timestamp     TIMESTAMP(6),
            event_type          STRING,
            mindfulness_session BOOLEAN,
            alert               BOOLEAN,
            happy               BOOLEAN,
            neutral             BOOLEAN,
            rested_relaxed      BOOLEAN,
            sad                 BOOLEAN,
            tense_anxious       BOOLEAN,
            tired               BOOLEAN,
            loc_entertainment   BOOLEAN,
            loc_gym             BOOLEAN,
            loc_home            BOOLEAN,
            loc_home_office     BOOLEAN,
            loc_other           BOOLEAN,
            loc_outdoors        BOOLEAN,
            loc_transit         BOOLEAN,
            loc_work_school     BOOLEAN,
            processed_at        TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    def _bool_flag(field: str) -> str:
        return f"(CAST(JSON_VALUE(payload, '$.{field}') AS FLOAT) > 0)"

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.context
        SELECT
            {_ENVELOPE},
            (UPPER(JSON_VALUE(payload, '$.mindfulness_session')) = 'TRUE') AS mindfulness_session,
            {_bool_flag('ALERT')}          AS alert,
            {_bool_flag('HAPPY')}          AS happy,
            {_bool_flag('NEUTRAL')}        AS neutral,
            {_bool_flag('RESTED_RELAXED')} AS rested_relaxed,
            {_bool_flag('SAD')}            AS sad,
            {_bool_flag('TENSE_ANXIOUS')}  AS tense_anxious,
            {_bool_flag('TIRED')}          AS tired,
            {_bool_flag('ENTERTAINMENT')}  AS loc_entertainment,
            {_bool_flag('GYM')}            AS loc_gym,
            {_bool_flag('HOME')}           AS loc_home,
            {_bool_flag('HOME_OFFICE')}    AS loc_home_office,
            {_bool_flag('OTHER')}          AS loc_other,
            {_bool_flag('OUTDOORS')}       AS loc_outdoors,
            {_bool_flag('TRANSIT')}        AS loc_transit,
            {_bool_flag('WORK_SCHOOL')}    AS loc_work_school,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('context')}
        WHERE {_BASE_WHERE}
    """)


# ---------------------------------------------------------------------------
# profile  — age and bmi are string categories ("<30", "<19"), NOT numeric
# ---------------------------------------------------------------------------
def process_profile() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.profile (
            user_id          STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date       DATE,
            event_hour       INT,
            event_timestamp  TIMESTAMP(6),
            event_type       STRING,
            badge_type       STRING,
            age              STRING,
            gender           STRING,
            bmi              STRING,
            step_goal        STRING,
            min_goal         FLOAT,
            max_goal         FLOAT,
            step_goal_label  STRING,
            processed_at     TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.profile
        SELECT
            {_ENVELOPE},
            JSON_VALUE(payload, '$.badgeType')       AS badge_type,
            JSON_VALUE(payload, '$.age')             AS age,
            JSON_VALUE(payload, '$.gender')          AS gender,
            JSON_VALUE(payload, '$.bmi')             AS bmi,
            JSON_VALUE(payload, '$.step_goal')       AS step_goal,
            CAST(JSON_VALUE(payload, '$.min_goal') AS FLOAT) AS min_goal,
            CAST(JSON_VALUE(payload, '$.max_goal') AS FLOAT) AS max_goal,
            JSON_VALUE(payload, '$.step_goal_label') AS step_goal_label,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('profile')}
        WHERE user_id IS NOT NULL
    """)


# ---------------------------------------------------------------------------
# sleep
# ---------------------------------------------------------------------------
def process_sleep() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.sleep (
            user_id                STRING,
            kafka_ingest_time      TIMESTAMP(6),
            event_date             DATE,
            event_hour             INT,
            event_timestamp        TIMESTAMP(6),
            event_type             STRING,
            sleep_duration_ms      FLOAT,
            minutes_to_fall_asleep FLOAT,
            minutes_asleep         FLOAT,
            minutes_awake          FLOAT,
            minutes_after_wakeup   FLOAT,
            sleep_efficiency       FLOAT,
            sleep_deep_ratio       FLOAT,
            sleep_wake_ratio       FLOAT,
            sleep_light_ratio      FLOAT,
            sleep_rem_ratio        FLOAT,
            processed_at           TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.sleep
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.sleep_duration')         AS FLOAT) AS sleep_duration_ms,
            CAST(JSON_VALUE(payload, '$.minutes_to_fall_asleep') AS FLOAT) AS minutes_to_fall_asleep,
            CAST(JSON_VALUE(payload, '$.minutes_asleep')         AS FLOAT) AS minutes_asleep,
            CAST(JSON_VALUE(payload, '$.minutes_awake')          AS FLOAT) AS minutes_awake,
            CAST(JSON_VALUE(payload, '$.minutes_after_wakeup')   AS FLOAT) AS minutes_after_wakeup,
            CAST(JSON_VALUE(payload, '$.sleep_efficiency')       AS FLOAT) AS sleep_efficiency,
            CAST(JSON_VALUE(payload, '$.sleep_deep_ratio')       AS FLOAT) AS sleep_deep_ratio,
            CAST(JSON_VALUE(payload, '$.sleep_wake_ratio')       AS FLOAT) AS sleep_wake_ratio,
            CAST(JSON_VALUE(payload, '$.sleep_light_ratio')      AS FLOAT) AS sleep_light_ratio,
            CAST(JSON_VALUE(payload, '$.sleep_rem_ratio')        AS FLOAT) AS sleep_rem_ratio,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('sleep')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.sleep_efficiency') IS NULL
               OR CAST(JSON_VALUE(payload, '$.sleep_efficiency') AS FLOAT) BETWEEN 0.0 AND 100.0)
          AND (JSON_VALUE(payload, '$.minutes_asleep') IS NULL
               OR CAST(JSON_VALUE(payload, '$.minutes_asleep')   AS FLOAT) >= 0)
    """)


# ---------------------------------------------------------------------------
# hrv_summary
# ---------------------------------------------------------------------------
def process_hrv_summary() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.hrv_summary (
            user_id           STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date        DATE,
            event_hour        INT,
            event_timestamp   TIMESTAMP(6),
            event_type        STRING,
            nremhr            FLOAT,
            rmssd             FLOAT,
            processed_at      TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.hrv_summary
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.nremhr') AS FLOAT) AS nremhr,
            CAST(JSON_VALUE(payload, '$.rmssd')  AS FLOAT) AS rmssd,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('hrv_summary')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.nremhr') IS NULL
               OR CAST(JSON_VALUE(payload, '$.nremhr') AS FLOAT) BETWEEN 30.0  AND 150.0)
          AND (JSON_VALUE(payload, '$.rmssd')  IS NULL
               OR CAST(JSON_VALUE(payload, '$.rmssd')  AS FLOAT) BETWEEN 0.0   AND 300.0)
    """)


# ---------------------------------------------------------------------------
# breathing_summary
# ---------------------------------------------------------------------------
def process_breathing_summary() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.breathing_summary (
            user_id           STRING,
            kafka_ingest_time TIMESTAMP(6),
            event_date        DATE,
            event_hour        INT,
            event_timestamp   TIMESTAMP(6),
            event_type        STRING,
            breathing_rate    FLOAT,
            processed_at      TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.breathing_summary
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.full_sleep_breathing_rate') AS FLOAT) AS breathing_rate,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('breathing_summary')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.full_sleep_breathing_rate') IS NULL
               OR CAST(JSON_VALUE(payload, '$.full_sleep_breathing_rate') AS FLOAT) BETWEEN 5.0 AND 40.0)
    """)


# ---------------------------------------------------------------------------
# vitals_daily
# ---------------------------------------------------------------------------
def process_vitals_daily() -> None:
    _create("""
        CREATE TABLE IF NOT EXISTS iceberg_cat.silver.vitals_daily (
            user_id                      STRING,
            kafka_ingest_time            TIMESTAMP(6),
            event_date                   DATE,
            event_hour                   INT,
            event_timestamp              TIMESTAMP(6),
            event_type                   STRING,
            spo2                         FLOAT,
            stress_score                 FLOAT,
            resting_hr                   FLOAT,
            vo2max                       FLOAT,
            nightly_temperature          FLOAT,
            daily_temperature_variation  FLOAT,
            sleep_points_pct             FLOAT,
            exertion_points_pct          FLOAT,
            responsiveness_points_pct    FLOAT,
            processed_at                 TIMESTAMP(6)
        ) PARTITIONED BY (event_date)
        WITH ('format-version' = '2')
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.silver.vitals_daily
        SELECT
            {_ENVELOPE},
            CAST(JSON_VALUE(payload, '$.spo2')                          AS FLOAT) AS spo2,
            CAST(JSON_VALUE(payload, '$.stress_score')                  AS FLOAT) AS stress_score,
            CAST(JSON_VALUE(payload, '$.resting_hr')                    AS FLOAT) AS resting_hr,
            CAST(JSON_VALUE(payload, '$.filtered_demographic_vo2max')   AS FLOAT) AS vo2max,
            CAST(JSON_VALUE(payload, '$.nightly_temperature')           AS FLOAT) AS nightly_temperature,
            CAST(JSON_VALUE(payload, '$.daily_temperature_variation')   AS FLOAT) AS daily_temperature_variation,
            CAST(JSON_VALUE(payload, '$.sleep_points_pct')              AS FLOAT) AS sleep_points_pct,
            CAST(JSON_VALUE(payload, '$.exertion_points_pct')           AS FLOAT) AS exertion_points_pct,
            CAST(JSON_VALUE(payload, '$.responsiveness_points_pct')     AS FLOAT) AS responsiveness_points_pct,
            CURRENT_TIMESTAMP AS processed_at
        FROM {_src('vitals_daily')}
        WHERE {_BASE_WHERE}
          AND (JSON_VALUE(payload, '$.spo2')         IS NULL
               OR CAST(JSON_VALUE(payload, '$.spo2')         AS FLOAT) BETWEEN 70.0  AND 100.0)
          AND (JSON_VALUE(payload, '$.resting_hr')   IS NULL
               OR CAST(JSON_VALUE(payload, '$.resting_hr')   AS FLOAT) BETWEEN 30.0  AND 150.0)
          AND (JSON_VALUE(payload, '$.stress_score') IS NULL
               OR CAST(JSON_VALUE(payload, '$.stress_score') AS FLOAT) BETWEEN 0.0   AND 100.0)
    """)


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
    selected = SILVER_ONLY or {
        "heart_rate_intraday",
        "hrv_intraday",
        "breathing_intraday",
    }
    print(f"[silver] Selected pipeline(s): {', '.join(sorted(selected))}")

    if _enabled("vitals"):
        process_vitals()
    if _enabled("activity"):
        process_activity()
    if _enabled("context"):
        process_context()
    if _enabled("profile"):
        process_profile()
    if _enabled("sleep"):
        process_sleep()
    if _enabled("hrv_summary"):
        process_hrv_summary()
    if _enabled("breathing_summary"):
        process_breathing_summary()
    if _enabled("vitals_daily"):
        process_vitals_daily()
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
