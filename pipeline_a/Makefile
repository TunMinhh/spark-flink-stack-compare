# =============================================================================
# CONFIG
# =============================================================================

PYTHON          ?= $(shell [ -f .venv/bin/python ] && echo .venv/bin/python || echo python3)
CSV_HOURLY      := ../data/hourly_fitbit_sema_df_unprocessed.csv
CSV_DAILY       := ../data/daily_fitbit_sema_df_unprocessed.csv
KAFKA_BOOTSTRAP := 127.0.0.1:9092

# Docker container IDs (resolved at runtime)
C_SPARK    := $(shell docker compose ps -q spark-master)
C_KAFKA    := $(shell docker compose ps -q kafka)
C_HDFS     := $(shell docker compose ps -q namenode)
C_FASTAPI  := $(shell docker compose ps -q fastapi)
C_PGSINK   := $(shell docker compose ps -q postgres-sink)
C_GRAFANA  := $(shell docker compose ps -q grafana)

# Spark packages (Maven coordinates)
PKG_KAFKA    := org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
PKG_DELTA    := io.delta:delta-spark_2.12:3.2.0
PKG_POSTGRES := org.postgresql:postgresql:42.7.3

# -----------------------------------------------------------------------------
# Spark memory - realistic single-pipeline profile for one 32-vCPU / 64-GB VM.
# Drivers run in spark-master; executors + overhead run in spark-worker.
#
#   Bronze :  2g exec + 1g overhead = 3g in worke
#   Silver :  4g exec + 2g overhead = 6g in worke
#   Gold   :  6g exec + 3g overhead = 9g in worke
#   Total Spark engine budget: ~18g across the three streaming jobs.
# -----------------------------------------------------------------------------
DRIVER_MEM := 1g

BRONZE_EXEC     := 2g
BRONZE_OVERHEAD := 1g

SILVER_EXEC     := 4g
SILVER_OVERHEAD := 2g

GOLD_EXEC     := 6g
GOLD_OVERHEAD := 3g

# Spark Standalone gives an app all available worker cores unless capped.
# These caps let Bronze, Silver, and Gold run concurrently on one VM.
BRONZE_CORES ?= 6
SILVER_CORES ?= 6
GOLD_CORES   ?= 6

# Kafka topics used by the realtime-only pipeline.
ALL_TOPICS := \
	wearable_heart_rate_intraday \
	wearable_hrv_intraday \
	wearable_breathing_intraday

# Realtime producer tuning  (req/s = USERS_PER_TICK × 3 / REALTIME_DELAY)
USERS_PER_TICK ?= 50
REALTIME_DELAY ?= 0.1

# Benchmark settings
# Override REQUEST_RATES for different load profiles, e.g.:
#   make benchmark REQUEST_RATES=1000 N_RUNS=1 WARMUP_RUNS=0
REQUEST_RATES  ?= 50,100,200
N_RUNS         ?= 3
WARMUP_RUNS    ?= 1
WARMUP_SECS    ?= 10

# Single-VM benchmark profile. Kafka partitions should be high enough to let
# Spark read in parallel, while shuffle stays modest to avoid many tiny Delta
# files and expensive metadata commits on single-node HDFS.
BRONZE_TOPICS ?= wearable_heart_rate_intraday,wearable_hrv_intraday,wearable_breathing_intraday
INTRADAY_TABLES := heart_rate_intraday,hrv_intraday,breathing_intraday
SILVER_ONLY ?= $(INTRADAY_TABLES)
GOLD_ONLY ?= daily_intraday_summary
KAFKA_PARTITIONS ?= 12
BRONZE_TRIGGER_SECONDS ?= 15
SILVER_TRIGGER_SECONDS ?= 15
GOLD_TRIGGER_SECONDS ?= 15
SHUFFLE_PARTITIONS ?= 18
SILVER_WRITE_COALESCE ?= 2
SPARK_MAX_RECORDS_PER_FILE ?= 50000

# =============================================================================
# PHONY
# =============================================================================

.PHONY: \
	init wait-hdfs init-hdfs init-kafka \
	producer-realtime \
	bronze silver gold export-gold \
	check-kafka check-hdfs check-silver check-gold \
	check-grafana check-pg-sink reset-pg-sink \
	train-ai build-fastapi pyspark \
	benchmark benchmark-quick smoke-streaming \
	reset reset-silve

# =============================================================================
# INIT
# =============================================================================

init: wait-hdfs init-hdfs init-kafka

wait-hdfs:
	@until docker exec $(C_HDFS) hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do \
		echo "[hdfs] waiting for NameNode safe mode to turn off..."; \
		sleep 5; \
	done

init-hdfs:
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/bronze/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/silver/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /data/gold/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /checkpoints/bronze/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /checkpoints/silver/wearable
	docker exec $(C_HDFS) hdfs dfs -mkdir -p /checkpoints/gold/wearable
	docker exec $(C_HDFS) hdfs dfs -chmod 777 /data /data/bronze /data/bronze/wearable /data/silver /data/silver/wearable /data/gold /data/gold/wearable
	docker exec $(C_HDFS) hdfs dfs -chmod 777 /checkpoints /checkpoints/bronze /checkpoints/bronze/wearable /checkpoints/silver /checkpoints/silver/wearable /checkpoints/gold /checkpoints/gold/wearable

init-kafka:
	docker exec $(C_KAFKA) bash -lc '\
		cd /opt/kafka/bin && \
		for t in $(ALL_TOPICS); do \
			./kafka-topics.sh --bootstrap-server localhost:19092 \
				--create --if-not-exists \
				--topic "$$t" --partitions $(KAFKA_PARTITIONS) --replication-factor 1; \
			./kafka-topics.sh --bootstrap-server localhost:19092 \
				--alter --if-exists \
				--topic "$$t" --partitions $(KAFKA_PARTITIONS) >/dev/null 2>&1 || true; \
		done'

# =============================================================================
# PRODUCER  (run on host, stream realtime intraday data into Kafka)
# =============================================================================


producer-realtime:
	HOURLY_CSV_PATH=$(CSV_HOURLY) \
	DAILY_CSV_PATH=$(CSV_DAILY) \
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	KAFKA_PARTITIONS=$(KAFKA_PARTITIONS) \
	USERS_PER_TICK=$(USERS_PER_TICK) \
	DELAY=$(REALTIME_DELAY) \
	$(PYTHON) ingestion/producer_realtime.py

# =============================================================================
# SPARK JOBS
# =============================================================================

# Shared spark-submit flags injected into every job
define SPARK_SUBMIT
	docker exec $(C_SPARK) sh -lc ' \
		mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && chmod -R 777 /tmp/.ivy2 && \
		BRONZE_TOPICS="$(BRONZE_TOPICS)" \
		SILVER_ONLY="$(SILVER_ONLY)" \
		GOLD_ONLY="$(GOLD_ONLY)" \
		SHUFFLE_PARTITIONS="$(SHUFFLE_PARTITIONS)" \
		BRONZE_TRIGGER_SECONDS="$(BRONZE_TRIGGER_SECONDS)" \
		SILVER_TRIGGER_SECONDS="$(SILVER_TRIGGER_SECONDS)" \
		GOLD_TRIGGER_SECONDS="$(GOLD_TRIGGER_SECONDS)" \
		SILVER_WRITE_COALESCE="$(SILVER_WRITE_COALESCE)" \
		SPARK_MAX_RECORDS_PER_FILE="$(SPARK_MAX_RECORDS_PER_FILE)" \
		exec /opt/spark/bin/spark-submit \
			--master spark://spark-master:7077 \
			--driver-memory $(DRIVER_MEM) \
			--conf spark.driverEnv.BRONZE_TOPICS="$(BRONZE_TOPICS)" \
			--conf spark.driverEnv.SILVER_ONLY="$(SILVER_ONLY)" \
			--conf spark.driverEnv.GOLD_ONLY="$(GOLD_ONLY)" \
			--conf spark.driverEnv.SHUFFLE_PARTITIONS="$(SHUFFLE_PARTITIONS)" \
			--conf spark.driverEnv.BRONZE_TRIGGER_SECONDS="$(BRONZE_TRIGGER_SECONDS)" \
			--conf spark.driverEnv.SILVER_TRIGGER_SECONDS="$(SILVER_TRIGGER_SECONDS)" \
			--conf spark.driverEnv.GOLD_TRIGGER_SECONDS="$(GOLD_TRIGGER_SECONDS)" \
			--conf spark.driverEnv.SILVER_WRITE_COALESCE="$(SILVER_WRITE_COALESCE)" \
			--conf spark.jars.ivy=/tmp/.ivy2 \
			--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
			--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
			--conf spark.sql.ansi.enabled=false \
			--conf spark.sql.adaptive.enabled=false \
			--conf spark.speculation=false \
			--conf spark.sql.files.maxRecordsPerFile=$(SPARK_MAX_RECORDS_PER_FILE) \
			--conf spark.sql.shuffle.partitions=$(SHUFFLE_PARTITIONS)
endef

# Bronze — streaming: Kafka → Bronze Delta
bronze: wait-hdfs init-hdfs init-kafka
	$(SPARK_SUBMIT) \
		--executor-cores $(BRONZE_CORES) \
		--conf spark.cores.max=$(BRONZE_CORES) \
		--executor-memory $(BRONZE_EXEC) \
		--conf spark.executor.memoryOverhead=$(BRONZE_OVERHEAD) \
		--packages $(PKG_KAFKA),$(PKG_DELTA) \
		/opt/spark/work-dir/spark_bronze.py'

# Silver - streaming: Bronze Delta -> Silver Delta for high-volume intraday data.
silver:
	$(SPARK_SUBMIT) \
		--executor-cores $(SILVER_CORES) \
		--conf spark.cores.max=$(SILVER_CORES) \
		--executor-memory $(SILVER_EXEC) \
		--conf spark.executor.memoryOverhead=$(SILVER_OVERHEAD) \
		--packages $(PKG_DELTA) \
		/opt/spark/work-dir/spark_silver_streaming.py'

# Gold - streaming: Silver Delta -> daily_intraday_summary Delta.
gold:
	$(SPARK_SUBMIT) \
		--executor-cores $(GOLD_CORES) \
		--conf spark.cores.max=$(GOLD_CORES) \
		--executor-memory $(GOLD_EXEC) \
		--conf spark.executor.memoryOverhead=$(GOLD_OVERHEAD) \
		--packages $(PKG_DELTA) \
		/opt/spark/work-dir/spark_gold_streaming.py'


# Phase 4 — Export Gold Delta tables -> PostgreSQL sink for Grafana.
#           Lightweight (small daily aggregates) — same memory budget as Silver.
export-gold:
	$(SPARK_SUBMIT) \
		--executor-memory $(SILVER_EXEC) \
		--conf spark.executor.memoryOverhead=$(SILVER_OVERHEAD) \
		--packages $(PKG_DELTA),$(PKG_POSTGRES) \
		--conf spark.driverEnv.PG_SINK_HOST=postgres-sink \
		/opt/spark/work-dir/spark_gold_to_postgres.py'

# =============================================================================
# CHECKS
# =============================================================================

check-kafka:
	docker exec $(C_KAFKA) bash -lc \
		'cd /opt/kafka/bin && ./kafka-topics.sh --bootstrap-server localhost:19092 --list'

check-hdfs:
	docker exec $(C_HDFS) hdfs dfs -ls /data/bronze/wearable

check-silver:
	docker exec $(C_HDFS) hdfs dfs -ls /data/silver/wearable

check-gold:
	docker exec $(C_HDFS) hdfs dfs -ls /data/gold/wearable

check-mlflow:
	@curl -sf http://localhost:5000/health && echo "[mlflow] OK" || echo "[mlflow] UNREACHABLE"
	@curl -sf http://localhost:5000/api/2.0/mlflow/experiments/list \
		| python3 -c "import sys,json; exps=json.load(sys.stdin).get('experiments',[]); \
		  print(f'[mlflow] {len(exps)} experiment(s): {[e[\"name\"] for e in exps]}')" 2>/dev/null || true

check-fastapi:
	@curl -sf http://localhost:8000/health | python3 -m json.tool || echo "[fastapi] UNREACHABLE"

check-grafana:
	@curl -sf http://localhost:3000/api/health | python3 -m json.tool || echo "[grafana] UNREACHABLE"

check-pg-sink:
	docker exec $(C_PGSINK) psql -U $${PG_SINK_USER:-grafana} -d $${PG_SINK_DB:-wellness} \
		-c "\dt" \
		-c "SELECT 'daily_intraday_summary' AS t, COUNT(*) FROM daily_intraday_summary UNION ALL \
		    SELECT 'ai_intraday_insights', COUNT(*) FROM ai_intraday_insights;"

reset-pg-sink:
	docker exec $(C_PGSINK) psql -U $${PG_SINK_USER:-grafana} -d $${PG_SINK_DB:-wellness} \
		-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	@echo "[reset] PostgreSQL sink cleared"

# Trigger AI training via FastAPI (non-blocking — returns job_id immediately)
train-ai:
	@curl -sf -X POST http://localhost:8000/train | python3 -m json.tool

# Build the FastAPI Docker image (run once before `docker compose up -d fastapi`)
build-fastapi:
	docker compose build fastapi

# Interactive PySpark shell with Delta Lake (spark-master must be running)
pyspark:
	docker exec -it $(C_SPARK) \
		/opt/spark/bin/pyspark \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--packages io.delta:delta-spark_2.12:3.2.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# =============================================================================
# RESET
# =============================================================================

reset:
	docker exec $(C_KAFKA) bash -lc '\
		cd /opt/kafka/bin && \
		for t in $$(./kafka-topics.sh --bootstrap-server localhost:19092 --list); do \
			./kafka-topics.sh --bootstrap-server localhost:19092 --delete --if-exists --topic "$$t"; \
		done'
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/bronze/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/silver/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/gold/wearable/*
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /checkpoints/*

reset-silver:
	docker exec $(C_HDFS) hdfs dfs -rm -r -f /data/silver/wearable/*
	@echo "[reset] Silver cleared — run: make silver"

# =============================================================================
# BENCHMARK
# =============================================================================

# Full benchmark: runs at REQUEST_RATES, N_RUNS times each
# Prerequisites: make bronze + make producer must have already run
benchmark:
	REQUEST_RATES=$(REQUEST_RATES) \
	N_RUNS=$(N_RUNS) \
	WARMUP_RUNS=$(WARMUP_RUNS) \
	WARMUP_SECS=$(WARMUP_SECS) \
	SHUFFLE_PARTITIONS=$(SHUFFLE_PARTITIONS) \
	BRONZE_TRIGGER_SECONDS=$(BRONZE_TRIGGER_SECONDS) \
	SILVER_TRIGGER_SECONDS=$(SILVER_TRIGGER_SECONDS) \
	GOLD_TRIGGER_SECONDS=$(GOLD_TRIGGER_SECONDS) \
	BRONZE_TOPICS=$(BRONZE_TOPICS) \
	SILVER_ONLY=$(SILVER_ONLY) \
	GOLD_ONLY=$(GOLD_ONLY) \
	bash benchmark/run_benchmark.sh

# Quick single-rate smoke test (50 users/tick, 1 run, 30s warmup)
benchmark-quick:
	REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 bash benchmark/run_benchmark.sh

# Lightweight preflight: send a tiny realtime burst and verify Bronze/Silver/Gold advance.
smoke-streaming:
	SMOKE_USERS_PER_TICK=1 SMOKE_SECS=10 SMOKE_WAIT_SECS=180 \
	$(PYTHON) benchmark/smoke_streaming.py
