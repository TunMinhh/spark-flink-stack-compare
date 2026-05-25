"""
Export layer: Gold Iceberg → PostgreSQL sink (batch)

Exports the realtime Gold tables used by the producer_realtime pipeline.
Uses JDBC overwrite; Gold is re-derived each run so there is no state to
preserve in the sink.

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
