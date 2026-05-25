"""
Gold layer: Silver Iceberg → Gold Iceberg (TRUE STREAMING)

Continuously running Flink streaming job. By default it maintains only the
realtime Gold table: daily_intraday_summary. It reads the three intraday Silver
Iceberg tables in streaming mode and upserts aggregates via Iceberg V2 equality
deletes (write.upsert.enabled=true).

The older full Gold builders remain in this file for explicit experiments via
GOLD_ONLY, but they are not part of the default producer_realtime pipeline.

Streaming semantics:
  - Unbounded GROUP BY emits an "update stream" (retract + emit on every new input)
  - Iceberg sink in upsert mode handles updates via equality deletes
  - Primary key MUST be declared on every Gold table (Iceberg requirement for upsert)
  - State grows with cardinality of (user_id, event_date) — bounded by ~users * ~days

Submit via:  `make gold`  → calls `flink run -py flink_gold.py …`
The script returns immediately after job submission; the job runs forever.

IMPORTANT: tables must NOT already exist with non-upsert schema. If you previously
ran the batch Gold, run `make reset` (or DROP the gold.* tables) before submitting
the streaming Gold job.
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

ICEBERG_URI = os.environ["ICEBERG_CATALOG_URI"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
AWS_KEY     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET  = os.environ["AWS_SECRET_ACCESS_KEY"]
PARALLELISM = int(os.environ.get("PARALLELISM", "6"))

MONITOR_INTERVAL = os.environ.get("ICEBERG_MONITOR_INTERVAL", "15s")
CHECKPOINT_INTERVAL = os.environ.get("FLINK_CHECKPOINT_INTERVAL", "15 s")
MINI_BATCH_INTERVAL = os.environ.get("FLINK_MINI_BATCH_INTERVAL", "5 s")
MINI_BATCH_SIZE = os.environ.get("FLINK_MINI_BATCH_SIZE", "5000")
GOLD_ONLY = {
    name.strip()
    for name in os.environ.get("GOLD_ONLY", "").split(",")
    if name.strip()
}

t_env = TableEnvironment.create(
    EnvironmentSettings.new_instance().in_streaming_mode().build()
)
t_env.get_config().set("parallelism.default", str(PARALLELISM))

cfg = t_env.get_config().get_configuration()
cfg.set_string("table.exec.resource.default-parallelism", str(PARALLELISM))
cfg.set_string("table.exec.iceberg.infer-source-parallelism.max", str(PARALLELISM))
cfg.set_string("table.exec.mini-batch.enabled", "true")
cfg.set_string("table.exec.mini-batch.allow-latency", MINI_BATCH_INTERVAL)
cfg.set_string("table.exec.mini-batch.size", MINI_BATCH_SIZE)

# ── Checkpointing (required for Iceberg upserts) ──────────────────────────────
cfg.set_string("execution.checkpointing.interval",        CHECKPOINT_INTERVAL)
cfg.set_string("execution.checkpointing.mode",            "EXACTLY_ONCE")
cfg.set_string("execution.checkpointing.timeout",         "10 min")
cfg.set_string("execution.checkpointing.min-pause",       "5 s")
cfg.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
cfg.set_string("state.checkpoints.dir",                   "s3://mlflow/flink-checkpoints/gold")
cfg.set_string("restart-strategy.type",                   "fixed-delay")
cfg.set_string("restart-strategy.fixed-delay.attempts",   "3")
cfg.set_string("restart-strategy.fixed-delay.delay",      "10 s")

# Allow regular (unbounded) joins by giving Flink an idle-state retention window.
# Without this, state from cold (user_id, event_date) keys would grow forever.
# 7 days is generous for a benchmark run; tune for production.
cfg.set_string("table.exec.state.ttl", "7 d")

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
t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_cat.gold")

stmt_set = t_env.create_statement_set()


def _enabled(table_name: str) -> bool:
    return not GOLD_ONLY or table_name in GOLD_ONLY


def _create(ddl: str) -> None:
    """Create an Iceberg table with retry logic.
    The H2 in-memory catalog inside iceberg-rest can fail with UncheckedSQLException
    when concurrent Iceberg snapshot commits (from Bronze/Silver jobs) race with
    DDL writes. Retrying with backoff resolves the transient conflict.
    """
    import time
    for attempt in range(10):
        try:
            t_env.execute_sql(ddl).wait()
            return
        except Exception as e:
            err = str(e)
            if "AlreadyExists" in err:
                return  # table already exists — IF NOT EXISTS succeeded
            if attempt < 9:
                print(f"[gold] _create attempt {attempt + 1} failed (H2 conflict?), retrying in 3s… {err[:120]}")
                time.sleep(3)
            else:
                raise


def _src(silver_table: str, namespace: str = "silver") -> str:
    """Streaming Iceberg source hint.
    INCREMENTAL_FROM_EARLIEST_SNAPSHOT = pure unbounded streaming from snapshot 0.
    Unlike TABLE_SCAN_THEN_INCREMENTAL (which creates a HYBRID bounded+unbounded plan
    requiring one slot per Silver Parquet file), this strategy keeps the entire job
    in a single pipelined region, so Flink needs only 1 slot regardless of file count.
    """
    return (
        f"iceberg_cat.{namespace}.{silver_table} "
        f"/*+ OPTIONS('streaming'='true', "
        f"'monitor-interval'='{MONITOR_INTERVAL}', "
        f"'starting-strategy'='INCREMENTAL_FROM_EARLIEST_SNAPSHOT') */"
    )


# Common upsert-table options for every Gold sink.
# - format-version=2 enables row-level deletes
# - write.upsert.enabled=true tells the Flink sink to emit equality deletes for updates
_UPSERT_OPTS = """
    WITH (
        'format-version'        = '2',
        'write.upsert.enabled'  = 'true'
    )"""


# ---------------------------------------------------------------------------
# 1. daily_vitals_summary
# ---------------------------------------------------------------------------
def build_daily_vitals_summary() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_vitals_summary (
            user_id               STRING NOT NULL,
            event_date            DATE   NOT NULL,
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
            gold_updated_at       TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_vitals_summary
        SELECT
            user_id,
            event_date,
            AVG(CAST(bpm AS DOUBLE))         AS avg_bpm,
            MIN(CAST(bpm AS DOUBLE))         AS min_bpm,
            MAX(CAST(bpm AS DOUBLE))         AS max_bpm,
            STDDEV_SAMP(CAST(bpm AS DOUBLE)) AS stddev_bpm,
            AVG(CAST(temperature AS DOUBLE)) AS avg_temperature,
            MIN(CAST(temperature AS DOUBLE)) AS min_temperature,
            MAX(CAST(temperature AS DOUBLE)) AS max_temperature,
            AVG(CAST(scl_avg AS DOUBLE))     AS avg_scl,
            MAX(CAST(scl_avg AS DOUBLE))     AS max_scl,
            COUNT(event_hour)                AS vitals_hours_recorded,
            CURRENT_TIMESTAMP                AS gold_updated_at
        FROM {_src('vitals')}
        WHERE user_id IS NOT NULL AND event_date IS NOT NULL
        GROUP BY user_id, event_date
    """)


# ---------------------------------------------------------------------------
# 2. daily_activity_summary
#
# Streaming notes:
#   - dominant_activity_type uses nested aggregation + Top-1 ROW_NUMBER pattern,
#     which Flink supports as a streaming "Top-N" operator.
#   - profile LEFT JOIN is a regular streaming join (profile is small and rarely
#     updated, so state stays tiny).
# ---------------------------------------------------------------------------
def build_daily_activity_summary() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_activity_summary (
            user_id                    STRING NOT NULL,
            event_date                 DATE   NOT NULL,
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
            gold_updated_at            TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_activity_summary
        WITH agg AS (
            SELECT
                user_id,
                event_date,
                CAST(SUM(steps)                AS INT)    AS total_steps,
                CAST(SUM(CAST(calories AS DOUBLE)) AS DOUBLE) AS total_calories,
                CAST(SUM(CAST(distance AS DOUBLE)) AS DOUBLE) AS total_distance_m,
                CAST(SUM(minutes_zone_1)       AS INT)   AS total_minutes_zone_1,
                CAST(SUM(minutes_zone_2)       AS INT)   AS total_minutes_zone_2,
                CAST(SUM(minutes_zone_3)       AS INT)   AS total_minutes_zone_3,
                CAST(SUM(minutes_below_zone_1) AS INT)   AS total_minutes_below_zone_1,
                COUNT(event_hour)                        AS activity_hours_recorded
            FROM {_src('activity')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        type_counts AS (
            SELECT user_id, event_date, activity_type, COUNT(*) AS cnt
            FROM {_src('activity')}
            WHERE activity_type IS NOT NULL
              AND user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date, activity_type
        ),
        dominant AS (
            SELECT user_id, event_date, activity_type AS dominant_activity_type
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, event_date
                        ORDER BY cnt DESC, activity_type
                    ) AS rn
                FROM type_counts
            ) WHERE rn = 1
        )
        SELECT
            a.user_id,
            a.event_date,
            a.total_steps,
            a.total_calories,
            a.total_distance_m,
            a.total_minutes_zone_1,
            a.total_minutes_zone_2,
            a.total_minutes_zone_3,
            a.total_minutes_below_zone_1,
            a.activity_hours_recorded,
            CAST(
                COALESCE(a.total_minutes_zone_1, 0)
                + COALESCE(a.total_minutes_zone_2, 0)
                + COALESCE(a.total_minutes_zone_3, 0)
            AS INT) AS total_active_minutes,
            d.dominant_activity_type,
            CASE
                WHEN p.step_goal IS NOT NULL
                THEN (CAST(a.total_steps AS FLOAT) >= CAST(p.step_goal AS FLOAT))
                ELSE NULL
            END AS goal_met_steps,
            CURRENT_TIMESTAMP AS gold_updated_at
        FROM agg a
        LEFT JOIN dominant d
            ON a.user_id = d.user_id AND a.event_date = d.event_date
        LEFT JOIN {_src('profile')} p
            ON a.user_id = p.user_id
    """)


# ---------------------------------------------------------------------------
# 3. daily_context_summary
# ---------------------------------------------------------------------------
def build_daily_context_summary() -> None:
    mood_cols = ["alert", "happy", "neutral", "rested_relaxed", "sad", "tense_anxious", "tired"]
    loc_cols  = ["loc_gym", "loc_home", "loc_work_school", "loc_outdoors", "loc_transit"]

    def _hours(col: str) -> str:
        return f"CAST(SUM(CAST({col} AS INT)) AS INT) AS hours_{col}"

    hours_exprs = ",\n            ".join(_hours(c) for c in mood_cols + loc_cols)

    mood_hour_cols = [f"COALESCE(hours_{c}, 0)" for c in mood_cols]
    greatest_mood  = f"GREATEST({', '.join(mood_hour_cols)})"

    def _mood_when(col: str) -> str:
        return f"WHEN COALESCE(hours_{col}, 0) = {greatest_mood} THEN '{col}'"

    dominant_mood_expr = (
        f"CASE WHEN {greatest_mood} = 0 THEN CAST(NULL AS STRING)\n            "
        + "\n            ".join(_mood_when(c) for c in mood_cols)
        + "\n            ELSE CAST(NULL AS STRING) END AS dominant_mood"
    )

    schema_mood_cols = "\n            ".join(f"hours_{c} INT," for c in mood_cols)
    schema_loc_cols  = "\n            ".join(f"hours_{c} INT," for c in loc_cols)

    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_context_summary (
            user_id              STRING NOT NULL,
            event_date           DATE   NOT NULL,
            {schema_mood_cols}
            {schema_loc_cols}
            dominant_mood        STRING,
            mindfulness_sessions INT,
            sema_readings_count  BIGINT,
            gold_updated_at      TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_context_summary
        WITH hourly AS (
            SELECT
                user_id,
                event_date,
                {hours_exprs},
                CAST(SUM(CAST(mindfulness_session AS INT)) AS INT) AS mindfulness_sessions,
                COUNT(event_hour) AS sema_readings_count
            FROM {_src('context')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        )
        SELECT
            user_id,
            event_date,
            {', '.join(f'hours_{c}' for c in mood_cols)},
            {', '.join(f'hours_{c}' for c in loc_cols)},
            {dominant_mood_expr},
            mindfulness_sessions,
            sema_readings_count,
            CURRENT_TIMESTAMP AS gold_updated_at
        FROM hourly
    """)


# ---------------------------------------------------------------------------
# 4. daily_sleep_summary
# ---------------------------------------------------------------------------
def build_daily_sleep_summary() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_sleep_summary (
            user_id                  STRING NOT NULL,
            event_date               DATE   NOT NULL,
            avg_sleep_duration_ms    DOUBLE,
            avg_sleep_efficiency     DOUBLE,
            avg_minutes_asleep       DOUBLE,
            avg_minutes_awake        DOUBLE,
            avg_sleep_deep_ratio     DOUBLE,
            avg_sleep_rem_ratio      DOUBLE,
            avg_sleep_light_ratio    DOUBLE,
            gold_updated_at          TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_sleep_summary
        SELECT
            user_id,
            event_date,
            AVG(CAST(sleep_duration_ms  AS DOUBLE)) AS avg_sleep_duration_ms,
            AVG(CAST(sleep_efficiency   AS DOUBLE)) AS avg_sleep_efficiency,
            AVG(CAST(minutes_asleep     AS DOUBLE)) AS avg_minutes_asleep,
            AVG(CAST(minutes_awake      AS DOUBLE)) AS avg_minutes_awake,
            AVG(CAST(sleep_deep_ratio   AS DOUBLE)) AS avg_sleep_deep_ratio,
            AVG(CAST(sleep_rem_ratio    AS DOUBLE)) AS avg_sleep_rem_ratio,
            AVG(CAST(sleep_light_ratio  AS DOUBLE)) AS avg_sleep_light_ratio,
            CURRENT_TIMESTAMP                       AS gold_updated_at
        FROM {_src('sleep')}
        WHERE user_id IS NOT NULL AND event_date IS NOT NULL
        GROUP BY user_id, event_date
    """)


# ---------------------------------------------------------------------------
# 5. daily_vitals_daily_summary
# ---------------------------------------------------------------------------
def build_daily_vitals_daily_summary() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_vitals_daily_summary (
            user_id                  STRING NOT NULL,
            event_date               DATE   NOT NULL,
            avg_spo2                 DOUBLE,
            avg_stress_score         DOUBLE,
            avg_resting_hr           DOUBLE,
            avg_vo2max               DOUBLE,
            avg_nightly_temperature  DOUBLE,
            avg_daily_temp_variation DOUBLE,
            gold_updated_at          TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_vitals_daily_summary
        SELECT
            user_id,
            event_date,
            AVG(CAST(spo2 AS DOUBLE))                        AS avg_spo2,
            AVG(CAST(stress_score AS DOUBLE))                AS avg_stress_score,
            AVG(CAST(resting_hr AS DOUBLE))                  AS avg_resting_hr,
            AVG(CAST(vo2max AS DOUBLE))                      AS avg_vo2max,
            AVG(CAST(nightly_temperature AS DOUBLE))         AS avg_nightly_temperature,
            AVG(CAST(daily_temperature_variation AS DOUBLE)) AS avg_daily_temp_variation,
            CURRENT_TIMESTAMP                                AS gold_updated_at
        FROM {_src('vitals_daily')}
        WHERE user_id IS NOT NULL AND event_date IS NOT NULL
        GROUP BY user_id, event_date
    """)


# ---------------------------------------------------------------------------
# 6. daily_intraday_summary
# ---------------------------------------------------------------------------
def build_daily_intraday_summary() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_intraday_summary (
            user_id                  STRING NOT NULL,
            event_date               DATE   NOT NULL,
            intraday_avg_bpm         DOUBLE,
            intraday_min_bpm         DOUBLE,
            intraday_max_bpm         DOUBLE,
            intraday_stddev_bpm      DOUBLE,
            intraday_hr_readings     BIGINT,
            intraday_avg_rmssd       DOUBLE,
            intraday_min_rmssd       DOUBLE,
            intraday_max_rmssd       DOUBLE,
            intraday_avg_breathing   DOUBLE,
            intraday_min_breathing   DOUBLE,
            intraday_max_breathing   DOUBLE,
            gold_updated_at          TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    # Single UNION ALL of raw rows from 3 intraday sources, then one GROUP BY.
    # AVG/MIN/MAX/STDDEV_SAMP/COUNT naturally ignore NULLs, so each metric
    # is computed only over its own source rows.
    # Avoids the spine+JOIN pattern (UNION distinct on streaming aggregations
    # causes tasks to finish immediately in Flink streaming mode).
    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_intraday_summary
        SELECT
            user_id,
            event_date,
            AVG(bpm)                AS intraday_avg_bpm,
            MIN(bpm)                AS intraday_min_bpm,
            MAX(bpm)                AS intraday_max_bpm,
            STDDEV_SAMP(bpm)        AS intraday_stddev_bpm,
            COUNT(bpm)              AS intraday_hr_readings,
            AVG(rmssd)              AS intraday_avg_rmssd,
            MIN(rmssd)              AS intraday_min_rmssd,
            MAX(rmssd)              AS intraday_max_rmssd,
            AVG(breaths_per_minute) AS intraday_avg_breathing,
            MIN(breaths_per_minute) AS intraday_min_breathing,
            MAX(breaths_per_minute) AS intraday_max_breathing,
            CURRENT_TIMESTAMP       AS gold_updated_at
        FROM (
            SELECT user_id, event_date,
                CAST(bpm AS DOUBLE)              AS bpm,
                CAST(NULL AS DOUBLE)             AS rmssd,
                CAST(NULL AS DOUBLE)             AS breaths_per_minute
            FROM {_src('heart_rate_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            UNION ALL
            SELECT user_id, event_date,
                CAST(NULL AS DOUBLE)             AS bpm,
                CAST(rmssd AS DOUBLE)            AS rmssd,
                CAST(NULL AS DOUBLE)             AS breaths_per_minute
            FROM {_src('hrv_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            UNION ALL
            SELECT user_id, event_date,
                CAST(NULL AS DOUBLE)             AS bpm,
                CAST(NULL AS DOUBLE)             AS rmssd,
                CAST(breaths_per_minute AS DOUBLE) AS breaths_per_minute
            FROM {_src('breathing_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
        ) combined
        GROUP BY user_id, event_date
    """)


# ---------------------------------------------------------------------------
# 7. daily_wellness_profile  — joins the 6 Gold tables + profile demographics.
#
# In streaming mode we re-read the source Silver tables and aggregate fresh
# (rather than reading the Gold tables) — this keeps the dependency tree of a
# single Flink job graph clean (no cycle / consistency races between the
# downstream sink and the upstream Gold sinks within the same statement set).
# ---------------------------------------------------------------------------
def build_daily_wellness_profile() -> None:
    _create(f"""
        CREATE TABLE IF NOT EXISTS iceberg_cat.gold.daily_wellness_profile (
            user_id                    STRING NOT NULL,
            event_date                 DATE   NOT NULL,
            avg_bpm                    DOUBLE,
            min_bpm                    DOUBLE,
            max_bpm                    DOUBLE,
            avg_temperature            DOUBLE,
            avg_scl                    DOUBLE,
            vitals_hours_recorded      BIGINT,
            total_steps                INT,
            total_calories             DOUBLE,
            total_active_minutes       INT,
            total_minutes_zone_2       INT,
            total_minutes_zone_3       INT,
            activity_hours_recorded    BIGINT,
            mindfulness_sessions       INT,
            hours_tense_anxious        INT,
            hours_sad                  INT,
            sema_readings_count        BIGINT,
            avg_sleep_duration_ms      DOUBLE,
            avg_sleep_efficiency       DOUBLE,
            avg_sleep_deep_ratio       DOUBLE,
            avg_sleep_rem_ratio        DOUBLE,
            intraday_avg_bpm           DOUBLE,
            intraday_stddev_bpm        DOUBLE,
            intraday_avg_rmssd         DOUBLE,
            intraday_avg_breathing     DOUBLE,
            intraday_hr_readings       BIGINT,
            avg_spo2                   DOUBLE,
            avg_stress_score           DOUBLE,
            avg_resting_hr             DOUBLE,
            avg_vo2max                 DOUBLE,
            age                        STRING,
            gender                     STRING,
            bmi                        STRING,
            gold_updated_at            TIMESTAMP(6),
            PRIMARY KEY (user_id, event_date) NOT ENFORCED
        ) PARTITIONED BY (event_date) {_UPSERT_OPTS}
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO iceberg_cat.gold.daily_wellness_profile
        WITH
        v AS (
            SELECT user_id, event_date,
                AVG(CAST(bpm AS DOUBLE))         AS avg_bpm,
                MIN(CAST(bpm AS DOUBLE))         AS min_bpm,
                MAX(CAST(bpm AS DOUBLE))         AS max_bpm,
                AVG(CAST(temperature AS DOUBLE)) AS avg_temperature,
                AVG(CAST(scl_avg AS DOUBLE))     AS avg_scl,
                COUNT(event_hour)                AS vitals_hours_recorded
            FROM {_src('vitals')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        a AS (
            SELECT user_id, event_date,
                CAST(SUM(steps) AS INT) AS total_steps,
                CAST(SUM(CAST(calories AS DOUBLE)) AS DOUBLE) AS total_calories,
                CAST(SUM(COALESCE(minutes_zone_1,0)
                       + COALESCE(minutes_zone_2,0)
                       + COALESCE(minutes_zone_3,0)) AS INT) AS total_active_minutes,
                CAST(SUM(minutes_zone_2) AS INT) AS total_minutes_zone_2,
                CAST(SUM(minutes_zone_3) AS INT) AS total_minutes_zone_3,
                COUNT(event_hour) AS activity_hours_recorded
            FROM {_src('activity')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        c AS (
            SELECT user_id, event_date,
                CAST(SUM(CAST(mindfulness_session AS INT)) AS INT) AS mindfulness_sessions,
                CAST(SUM(CAST(tense_anxious AS INT)) AS INT)       AS hours_tense_anxious,
                CAST(SUM(CAST(sad AS INT))           AS INT)       AS hours_sad,
                COUNT(event_hour) AS sema_readings_count
            FROM {_src('context')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        sl AS (
            SELECT user_id, event_date,
                AVG(CAST(sleep_duration_ms AS DOUBLE)) AS avg_sleep_duration_ms,
                AVG(CAST(sleep_efficiency  AS DOUBLE)) AS avg_sleep_efficiency,
                AVG(CAST(sleep_deep_ratio  AS DOUBLE)) AS avg_sleep_deep_ratio,
                AVG(CAST(sleep_rem_ratio   AS DOUBLE)) AS avg_sleep_rem_ratio
            FROM {_src('sleep')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        hr AS (
            SELECT user_id, event_date,
                AVG(CAST(bpm AS DOUBLE))         AS intraday_avg_bpm,
                STDDEV_SAMP(CAST(bpm AS DOUBLE)) AS intraday_stddev_bpm,
                COUNT(bpm)                       AS intraday_hr_readings
            FROM {_src('heart_rate_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        hrv AS (
            SELECT user_id, event_date,
                AVG(CAST(rmssd AS DOUBLE)) AS intraday_avg_rmssd
            FROM {_src('hrv_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        br AS (
            SELECT user_id, event_date,
                AVG(CAST(breaths_per_minute AS DOUBLE)) AS intraday_avg_breathing
            FROM {_src('breathing_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        ),
        vd AS (
            SELECT user_id, event_date,
                AVG(CAST(spo2 AS DOUBLE))         AS avg_spo2,
                AVG(CAST(stress_score AS DOUBLE)) AS avg_stress_score,
                AVG(CAST(resting_hr AS DOUBLE))   AS avg_resting_hr,
                AVG(CAST(vo2max AS DOUBLE))       AS avg_vo2max
            FROM {_src('vitals_daily')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            GROUP BY user_id, event_date
        )
        SELECT
            v.user_id,
            v.event_date,
            v.avg_bpm, v.min_bpm, v.max_bpm, v.avg_temperature, v.avg_scl,
            v.vitals_hours_recorded,
            a.total_steps, a.total_calories, a.total_active_minutes,
            a.total_minutes_zone_2, a.total_minutes_zone_3,
            a.activity_hours_recorded,
            c.mindfulness_sessions, c.hours_tense_anxious, c.hours_sad,
            c.sema_readings_count,
            sl.avg_sleep_duration_ms, sl.avg_sleep_efficiency,
            sl.avg_sleep_deep_ratio, sl.avg_sleep_rem_ratio,
            hr.intraday_avg_bpm, hr.intraday_stddev_bpm,
            hrv.intraday_avg_rmssd, br.intraday_avg_breathing,
            hr.intraday_hr_readings,
            vd.avg_spo2, vd.avg_stress_score, vd.avg_resting_hr, vd.avg_vo2max,
            p.age, p.gender, p.bmi,
            CURRENT_TIMESTAMP AS gold_updated_at
        FROM v
        LEFT JOIN a   ON v.user_id = a.user_id   AND v.event_date = a.event_date
        LEFT JOIN c   ON v.user_id = c.user_id   AND v.event_date = c.event_date
        LEFT JOIN sl  ON v.user_id = sl.user_id  AND v.event_date = sl.event_date
        LEFT JOIN hr  ON v.user_id = hr.user_id  AND v.event_date = hr.event_date
        LEFT JOIN hrv ON v.user_id = hrv.user_id AND v.event_date = hrv.event_date
        LEFT JOIN br  ON v.user_id = br.user_id  AND v.event_date = br.event_date
        LEFT JOIN vd  ON v.user_id = vd.user_id  AND v.event_date = vd.event_date
        LEFT JOIN {_src('profile')} p ON v.user_id = p.user_id
    """)


# ---------------------------------------------------------------------------
# Main - submit ONE streaming job for the selected Gold table(s).
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    selected = GOLD_ONLY or {"daily_intraday_summary"}
    print(f"[gold] Selected aggregation(s): {', '.join(sorted(selected))}")

    if _enabled("daily_vitals_summary"):
        build_daily_vitals_summary()
    if _enabled("daily_activity_summary"):
        build_daily_activity_summary()
    if _enabled("daily_context_summary"):
        build_daily_context_summary()
    if _enabled("daily_sleep_summary"):
        build_daily_sleep_summary()
    if _enabled("daily_vitals_daily_summary"):
        build_daily_vitals_daily_summary()
    if _enabled("daily_intraday_summary"):
        build_daily_intraday_summary()
    if _enabled("daily_wellness_profile"):
        build_daily_wellness_profile()

    print("[gold] Submitting streaming job (unbounded aggregations + upsert sinks) …")
    stmt_set.execute()
    print("[gold] Job submitted. It runs forever until cancelled.")
    print("[gold] Monitor via Flink UI at http://localhost:8082/")
