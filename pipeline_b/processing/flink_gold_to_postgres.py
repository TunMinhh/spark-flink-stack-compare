"""
Export layer: Gold Iceberg → PostgreSQL sink (batch)

Exports the same 8 tables as Spark's spark_gold_to_postgres.py, with matching
table names. Uses JDBC overwrite (truncate + insert) — Gold is fully
re-derived each run so there is no state to preserve in the sink.

Run via:  make export-gold
"""

import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment

ICEBERG_URI = os.environ["ICEBERG_CATALOG_URI"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
AWS_KEY     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET  = os.environ["AWS_SECRET_ACCESS_KEY"]
PARALLELISM = int(os.environ.get("PARALLELISM", "2"))

PG_HOST = os.environ.get("PG_SINK_HOST", "postgres-sink")
PG_PORT = os.environ.get("PG_SINK_PORT", "5432")
PG_DB   = os.environ["PG_SINK_DB"]
PG_USER = os.environ["PG_SINK_USER"]
PG_PASS = os.environ["PG_SINK_PASSWORD"]
PG_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

t_env = TableEnvironment.create(
    EnvironmentSettings.new_instance().in_batch_mode().build()
)
t_env.get_config().set("parallelism.default", str(PARALLELISM))

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


def _check_exists(gold_table: str) -> bool:
    try:
        t_env.execute_sql(
            f"SELECT 1 FROM iceberg_cat.gold.{gold_table} LIMIT 1"
        ).wait()
        return True
    except Exception:
        return False


def export_table(gold_table: str, pg_table: str, sink_ddl_cols: str) -> None:
    if not _check_exists(gold_table):
        print(f"[export] {gold_table} — Gold table missing, skipping.")
        return

    sink = f"pg_sink_{pg_table}"
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE {sink} (
            {sink_ddl_cols}
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{PG_URL}',
            'table-name' = '{pg_table}',
            'username'   = '{PG_USER}',
            'password'   = '{PG_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """).wait()

    t_env.execute_sql(f"""
        INSERT INTO {sink}
        SELECT * FROM iceberg_cat.gold.{gold_table}
    """).wait()
    print(f"[export] {gold_table} → {pg_table}")

if os.environ.get("REALTIME_ONLY_EXPORT", "1") == "1":
    export_table("daily_intraday_summary", "daily_intraday_summary", """
        user_id                STRING,
        event_date             DATE,
        intraday_avg_bpm       DOUBLE,
        intraday_min_bpm       DOUBLE,
        intraday_max_bpm       DOUBLE,
        intraday_stddev_bpm    DOUBLE,
        intraday_hr_readings   BIGINT,
        intraday_avg_rmssd     DOUBLE,
        intraday_min_rmssd     DOUBLE,
        intraday_max_rmssd     DOUBLE,
        intraday_avg_breathing DOUBLE,
        intraday_min_breathing DOUBLE,
        intraday_max_breathing DOUBLE,
        gold_updated_at        TIMESTAMP(6)
    """)
    export_table("ai_intraday_insights", "ai_intraday_insights", """
        user_id              STRING,
        event_date           STRING,
        event_timestamp      TIMESTAMP(6),
        reconstruction_error FLOAT,
        z_score              FLOAT,
        severity             STRING,
        mlflow_run_id        STRING
    """)
    print("[export] Realtime Gold tables exported to postgres-sink.")
    sys.exit(0)


# ---------------------------------------------------------------------------
# 1. daily_vitals_summary
# ---------------------------------------------------------------------------
export_table("daily_vitals_summary", "daily_vitals_summary", """
    user_id               STRING,
    event_date            DATE,
    avg_bpm               DOUBLE,
    min_bpm               DOUBLE,
    max_bpm               DOUBLE,
    stddev_bpm            DOUBLE,
    avg_temperature       DOUBLE,
    min_temperature       DOUBLE,
    max_temperature       DOUBLE,
    avg_scl               DOUBLE,
    max_scl               DOUBLE,
    vitals_hours_recorded BIGINT,
    gold_updated_at       TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 2. daily_activity_summary
# ---------------------------------------------------------------------------
export_table("daily_activity_summary", "daily_activity_summary", """
    user_id                    STRING,
    event_date                 DATE,
    total_steps                INT,
    total_calories             DOUBLE,
    total_distance_m           DOUBLE,
    total_minutes_zone_1       INT,
    total_minutes_zone_2       INT,
    total_minutes_zone_3       INT,
    total_minutes_below_zone_1 INT,
    activity_hours_recorded    BIGINT,
    total_active_minutes       INT,
    dominant_activity_type     STRING,
    goal_met_steps             BOOLEAN,
    gold_updated_at            TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 3. daily_context_summary
# ---------------------------------------------------------------------------
export_table("daily_context_summary", "daily_context_summary", """
    user_id              STRING,
    event_date           DATE,
    hours_alert          INT,
    hours_happy          INT,
    hours_neutral        INT,
    hours_rested_relaxed INT,
    hours_sad            INT,
    hours_tense_anxious  INT,
    hours_tired          INT,
    hours_loc_gym        INT,
    hours_loc_home       INT,
    hours_loc_work_school INT,
    hours_loc_outdoors   INT,
    hours_loc_transit    INT,
    dominant_mood        STRING,
    mindfulness_sessions INT,
    sema_readings_count  BIGINT,
    gold_updated_at      TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 4. daily_sleep_summary
# ---------------------------------------------------------------------------
export_table("daily_sleep_summary", "daily_sleep_summary", """
    user_id               STRING,
    event_date            DATE,
    avg_sleep_duration_ms DOUBLE,
    avg_sleep_efficiency  DOUBLE,
    avg_minutes_asleep    DOUBLE,
    avg_minutes_awake     DOUBLE,
    avg_sleep_deep_ratio  DOUBLE,
    avg_sleep_rem_ratio   DOUBLE,
    avg_sleep_light_ratio DOUBLE,
    gold_updated_at       TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 5. daily_vitals_daily_summary
# ---------------------------------------------------------------------------
export_table("daily_vitals_daily_summary", "daily_vitals_daily_summary", """
    user_id                  STRING,
    event_date               DATE,
    avg_spo2                 DOUBLE,
    avg_stress_score         DOUBLE,
    avg_resting_hr           DOUBLE,
    avg_vo2max               DOUBLE,
    avg_nightly_temperature  DOUBLE,
    avg_daily_temp_variation DOUBLE,
    gold_updated_at          TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 6. daily_intraday_summary
# ---------------------------------------------------------------------------
export_table("daily_intraday_summary", "daily_intraday_summary", """
    user_id                STRING,
    event_date             DATE,
    intraday_avg_bpm       DOUBLE,
    intraday_min_bpm       DOUBLE,
    intraday_max_bpm       DOUBLE,
    intraday_stddev_bpm    DOUBLE,
    intraday_hr_readings   BIGINT,
    intraday_avg_rmssd     DOUBLE,
    intraday_min_rmssd     DOUBLE,
    intraday_max_rmssd     DOUBLE,
    intraday_avg_breathing DOUBLE,
    intraday_min_breathing DOUBLE,
    intraday_max_breathing DOUBLE,
    gold_updated_at        TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 7. daily_wellness_profile
# ---------------------------------------------------------------------------
export_table("daily_wellness_profile", "daily_wellness_profile", """
    user_id                 STRING,
    event_date              DATE,
    avg_bpm                 DOUBLE,
    min_bpm                 DOUBLE,
    max_bpm                 DOUBLE,
    avg_temperature         DOUBLE,
    avg_scl                 DOUBLE,
    vitals_hours_recorded   BIGINT,
    total_steps             INT,
    total_calories          DOUBLE,
    total_active_minutes    INT,
    total_minutes_zone_2    INT,
    total_minutes_zone_3    INT,
    goal_met_steps          BOOLEAN,
    activity_hours_recorded BIGINT,
    dominant_mood           STRING,
    mindfulness_sessions    INT,
    hours_tense_anxious     INT,
    hours_sad               INT,
    sema_readings_count     BIGINT,
    avg_sleep_duration_ms   DOUBLE,
    avg_sleep_efficiency    DOUBLE,
    avg_sleep_deep_ratio    DOUBLE,
    avg_sleep_rem_ratio     DOUBLE,
    intraday_avg_bpm        DOUBLE,
    intraday_stddev_bpm     DOUBLE,
    intraday_avg_rmssd      DOUBLE,
    intraday_avg_breathing  DOUBLE,
    intraday_hr_readings    BIGINT,
    avg_spo2                DOUBLE,
    avg_stress_score        DOUBLE,
    avg_resting_hr          DOUBLE,
    avg_vo2max              DOUBLE,
    age                     STRING,
    gender                  STRING,
    bmi                     STRING,
    gold_updated_at         TIMESTAMP(6)
""")

# ---------------------------------------------------------------------------
# 8. ai_insights  (written by the FastAPI ML module, not by the pipeline)
# ---------------------------------------------------------------------------
export_table("ai_insights", "ai_insights", """
    user_id               STRING,
    event_date            DATE,
    reconstruction_error  DOUBLE,
    z_score               DOUBLE,
    severity              STRING,
    mlflow_run_id         STRING
""")

print("[export] All Gold tables exported to postgres-sink.")
