"""
Gold layer: Silver Iceberg -> Gold Iceberg (realtime-only streaming).

Maintains the realtime Gold table `daily_intraday_summary` from the three
intraday Silver tables:
  - heart_rate_intraday
  - hrv_intraday
  - breathing_intraday

Submit via `make gold`. The Flink job is detached and runs until cancelled.
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

ICEBERG_URI = os.environ["ICEBERG_CATALOG_URI"]
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")
PARALLELISM = int(os.environ.get("PARALLELISM", "6"))

MONITOR_INTERVAL = os.environ.get("ICEBERG_MONITOR_INTERVAL", "15s")
CHECKPOINT_INTERVAL = os.environ.get("FLINK_CHECKPOINT_INTERVAL", "15 s")
MINI_BATCH_INTERVAL = os.environ.get("FLINK_MINI_BATCH_INTERVAL", "5 s")
MINI_BATCH_SIZE = os.environ.get("FLINK_MINI_BATCH_SIZE", "5000")

REALTIME_GOLD_TABLES = {"daily_intraday_summary"}
REQUESTED_GOLD = {
    name.strip()
    for name in os.environ.get("GOLD_ONLY", "").split(",")
    if name.strip()
}
IGNORED_GOLD = REQUESTED_GOLD - REALTIME_GOLD_TABLES
GOLD_SELECTED = (REQUESTED_GOLD & REALTIME_GOLD_TABLES) or REALTIME_GOLD_TABLES

t_env = TableEnvironment.create(
    EnvironmentSettings.new_instance().in_streaming_mode().build()
)
t_env.get_config().set("parallelism.default", str(PARALLELISM))

cfg = t_env.get_config().get_configuration()
cfg.set_string("execution.checkpointing.interval", CHECKPOINT_INTERVAL)
cfg.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
cfg.set_string("execution.checkpointing.timeout", "10 min")
cfg.set_string("execution.checkpointing.min-pause", "5 s")
cfg.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
cfg.set_string("state.checkpoints.dir", os.environ.get("FLINK_CHECKPOINT_DIR_GOLD", "hdfs://namenode:9000/checkpoints/flink/gold"))
cfg.set_string("restart-strategy.type", "fixed-delay")
cfg.set_string("restart-strategy.fixed-delay.attempts", "3")
cfg.set_string("restart-strategy.fixed-delay.delay", "10 s")
cfg.set_string("table.exec.mini-batch.enabled", "true")
cfg.set_string("table.exec.mini-batch.allow-latency", MINI_BATCH_INTERVAL)
cfg.set_string("table.exec.mini-batch.size", MINI_BATCH_SIZE)
cfg.set_string("fs.default-scheme", "hdfs://namenode:9000")
cfg.set_string("fs.hdfs.hadoopconf", "/opt/hadoop/etc/hadoop")

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

t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_cat.gold")
stmt_set = t_env.create_statement_set()

_UPSERT_OPTS = """
WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
)
"""


def _enabled(table_name: str) -> bool:
    return table_name in GOLD_SELECTED


def _create(ddl: str) -> None:
    t_env.execute_sql(ddl).wait()


def _src(silver_table: str) -> str:
    return (
        f"iceberg_cat.silver.{silver_table} "
        f"/*+ OPTIONS('streaming'='true', "
        f"'monitor-interval'='{MONITOR_INTERVAL}', "
        f"'starting-strategy'='INCREMENTAL_FROM_EARLIEST_SNAPSHOT') */"
    )


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
                CAST(bpm AS DOUBLE) AS bpm,
                CAST(NULL AS DOUBLE) AS rmssd,
                CAST(NULL AS DOUBLE) AS breaths_per_minute
            FROM {_src('heart_rate_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            UNION ALL
            SELECT user_id, event_date,
                CAST(NULL AS DOUBLE) AS bpm,
                CAST(rmssd AS DOUBLE) AS rmssd,
                CAST(NULL AS DOUBLE) AS breaths_per_minute
            FROM {_src('hrv_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
            UNION ALL
            SELECT user_id, event_date,
                CAST(NULL AS DOUBLE) AS bpm,
                CAST(NULL AS DOUBLE) AS rmssd,
                CAST(breaths_per_minute AS DOUBLE) AS breaths_per_minute
            FROM {_src('breathing_intraday')}
            WHERE user_id IS NOT NULL AND event_date IS NOT NULL
        ) combined
        GROUP BY user_id, event_date
    """)


if __name__ == "__main__":
    if IGNORED_GOLD:
        print(f"[gold] Ignoring non-realtime aggregation(s): {', '.join(sorted(IGNORED_GOLD))}")
    print(f"[gold] Selected realtime aggregation(s): {', '.join(sorted(GOLD_SELECTED))}")

    if _enabled("daily_intraday_summary"):
        build_daily_intraday_summary()

    print("[gold] Submitting streaming job (daily_intraday_summary) ...")
    stmt_set.execute()
    print("[gold] Job submitted. Monitor via Flink UI at http://localhost:8082/")
