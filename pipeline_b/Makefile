# =============================================================================
# CONFIG
# =============================================================================

PYTHON          := $(shell [ -f .venv/bin/python ] && echo .venv/bin/python || echo python3)
# CSV files are shared from the repository-level data directory.
CSV_HOURLY      := ../data/hourly_fitbit_sema_df_unprocessed.csv
CSV_DAILY       := ../data/daily_fitbit_sema_df_unprocessed.csv
KAFKA_BOOTSTRAP := 127.0.0.1:9092

# Docker container IDs (resolved at runtime)
C_FLINK    := $(shell docker compose ps -q flink-jobmanager)
C_KAFKA    := $(shell docker compose ps -q kafka)
C_NAMENODE := $(shell docker compose ps -q namenode)
C_FASTAPI  := $(shell docker compose ps -q fastapi)
C_PGSINK   := $(shell docker compose ps -q postgres-sink)
C_GRAFANA  := $(shell docker compose ps -q grafana)
C_MINIO    := $(shell docker compose ps -q minio)

# Iceberg + HDFS config (all Flink jobs read these from env)
ICEBERG_CATALOG_URI := http://iceberg-rest:8181
ICEBERG_WAREHOUSE   := hdfs://namenode:9000/warehouse/iceberg
MINIO_USER          ?= minioadmin
MINIO_PASS          ?= minioadmin123
PG_SINK_DB          ?= wellness
PG_SINK_USER        ?= grafana
PG_SINK_PASSWORD    ?= grafana123

# Kafka topics used by the realtime-only pipeline.
ALL_TOPICS := \
	wearable_heart_rate_intraday \
	wearable_hrv_intraday \
	wearable_breathing_intraday

# Flink parallelism and benchmark scope.
# Pipeline B benchmarks the realtime intraday path by default:
# Kafka intraday topics -> Bronze intraday -> Silver intraday -> Gold daily_intraday_summary.
# Realistic single-pipeline 32-vCPU / 64-GB benchmark profile. Three concurrent
# Flink jobs use PARALLELISM=6 each, so the TaskManager exposes 18 slots.
# Override PARALLELISM for scale-up experiments.
PARALLELISM ?= 6
INTRADAY_TABLES := heart_rate_intraday,hrv_intraday,breathing_intraday
BRONZE_ONLY ?= $(INTRADAY_TABLES)
SILVER_ONLY ?= $(INTRADAY_TABLES)
GOLD_ONLY ?= daily_intraday_summary

# Realtime producer tuning
USERS_PER_TICK ?= 50
REALTIME_DELAY ?= 0.1

# Benchmark settings
REQUEST_RATES  ?= 50,100,200
N_RUNS         ?= 3
WARMUP_RUNS    ?= 1
WARMUP_SECS    ?= 10
KAFKA_PARTITIONS ?= 12
ICEBERG_MONITOR_INTERVAL ?= 15s
FLINK_CHECKPOINT_INTERVAL ?= 15 s
FLINK_CHECKPOINT_INTERVAL_SECONDS ?= 15
FLINK_CHECKPOINT_MIN_PAUSE ?= 5 s
FLINK_MINI_BATCH_INTERVAL ?= 5 s
FLINK_MINI_BATCH_SIZE ?= 5000
GOLD_CHECKPOINT_SECONDS ?= 15

# =============================================================================
# PHONY
# =============================================================================

.PHONY: \
	init init-hdfs init-kafka init-iceberg \
	producer-realtime \
	bronze silver gold export-gold \
	cancel-bronze cancel-silver cancel-gold cancel-all \
	list-jobs \
	check-kafka check-iceberg check-silver check-gold \
	check-grafana check-pg-sink reset-pg-sink \
	train-ai \
	build-flink \
	benchmark benchmark-quick \
	reset reset-silver reset-gold

# =============================================================================
# INIT
# =============================================================================

init: init-hdfs init-kafka init-iceberg

init-hdfs:
	docker exec $(C_NAMENODE) hdfs dfs -mkdir -p /warehouse/iceberg
	docker exec $(C_NAMENODE) hdfs dfs -mkdir -p /checkpoints/flink/bronze
	docker exec $(C_NAMENODE) hdfs dfs -mkdir -p /checkpoints/flink/silver
	docker exec $(C_NAMENODE) hdfs dfs -mkdir -p /checkpoints/flink/gold
	docker exec $(C_NAMENODE) hdfs dfs -chmod -R 777 /warehouse /checkpoints
	@echo "[init-hdfs] HDFS warehouse/checkpoints ready"

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

# Create Iceberg namespaces for bronze / silver / gold via REST catalog API
init-iceberg:
	curl -sf -X POST http://localhost:8181/v1/namespaces \
		-H "Content-Type: application/json" \
		-d '{"namespace":["bronze"]}' || true
	curl -sf -X POST http://localhost:8181/v1/namespaces \
		-H "Content-Type: application/json" \
		-d '{"namespace":["silver"]}' || true
	curl -sf -X POST http://localhost:8181/v1/namespaces \
		-H "Content-Type: application/json" \
		-d '{"namespace":["gold"]}' || true
	@echo "[init-iceberg] namespaces ready: bronze, silver, gold"

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
# FLINK JOBS
# =============================================================================

# Shared env injected into every flink job via docker exec
define FLINK_ENV
	KAFKA_BOOTSTRAP=kafka:19092 \
	ICEBERG_CATALOG_URI=$(ICEBERG_CATALOG_URI) \
	ICEBERG_WAREHOUSE=$(ICEBERG_WAREHOUSE) \
	PARALLELISM=$(PARALLELISM) \
	ICEBERG_MONITOR_INTERVAL="$(ICEBERG_MONITOR_INTERVAL)" \
	FLINK_CHECKPOINT_INTERVAL="$(FLINK_CHECKPOINT_INTERVAL)" \
	FLINK_CHECKPOINT_INTERVAL_SECONDS="$(FLINK_CHECKPOINT_INTERVAL_SECONDS)" \
	FLINK_MINI_BATCH_INTERVAL="$(FLINK_MINI_BATCH_INTERVAL)" \
	FLINK_MINI_BATCH_SIZE="$(FLINK_MINI_BATCH_SIZE)"
endef

# Bronze — TRUE STREAMING: Kafka → Iceberg Bronze
# Submitted detached (-d). Job runs forever on the Flink cluster until cancelled.
bronze: init-hdfs init-kafka init-iceberg
	docker exec \
		-e KAFKA_BOOTSTRAP=kafka:19092 \
		-e ICEBERG_CATALOG_URI=$(ICEBERG_CATALOG_URI) \
		-e ICEBERG_WAREHOUSE=$(ICEBERG_WAREHOUSE) \
		-e PARALLELISM=$(PARALLELISM) \
		-e FLINK_CHECKPOINT_INTERVAL="$(FLINK_CHECKPOINT_INTERVAL)" \
		-e FLINK_CHECKPOINT_INTERVAL_SECONDS="$(FLINK_CHECKPOINT_INTERVAL_SECONDS)" \
		-e BRONZE_ONLY=$(BRONZE_ONLY) \
		$(C_FLINK) /opt/flink/bin/flink run -d -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3 -py /opt/flink/work-dir/flink_bronze.py

# Silver — TRUE STREAMING: Bronze Iceberg → Silver Iceberg (append-only, 3 intraday sources/sinks)
# Submitted detached. Cancel via `make cancel-silver` or `make cancel-all`.
silver:
	docker exec \
		-e ICEBERG_CATALOG_URI=$(ICEBERG_CATALOG_URI) \
		-e ICEBERG_WAREHOUSE=$(ICEBERG_WAREHOUSE) \
		-e PARALLELISM=$(PARALLELISM) \
		-e ICEBERG_MONITOR_INTERVAL="$(ICEBERG_MONITOR_INTERVAL)" \
		-e FLINK_CHECKPOINT_INTERVAL="$(FLINK_CHECKPOINT_INTERVAL)" \
		-e FLINK_CHECKPOINT_INTERVAL_SECONDS="$(FLINK_CHECKPOINT_INTERVAL_SECONDS)" \
		-e FLINK_CHECKPOINT_MIN_PAUSE="$(FLINK_CHECKPOINT_MIN_PAUSE)" \
		-e SILVER_ONLY=$(SILVER_ONLY) \
		$(C_FLINK) /opt/flink/bin/flink run -d -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3 -py /opt/flink/work-dir/flink_silver.py

# Gold - TRUE STREAMING: Silver Iceberg -> daily_intraday_summary (Iceberg upsert)
# Submitted detached.
gold:
	docker exec \
		-e ICEBERG_CATALOG_URI=$(ICEBERG_CATALOG_URI) \
		-e ICEBERG_WAREHOUSE=$(ICEBERG_WAREHOUSE) \
		-e PARALLELISM=$(PARALLELISM) \
		-e ICEBERG_MONITOR_INTERVAL="$(ICEBERG_MONITOR_INTERVAL)" \
		-e FLINK_CHECKPOINT_INTERVAL="$(FLINK_CHECKPOINT_INTERVAL)" \
		-e FLINK_CHECKPOINT_INTERVAL_SECONDS="$(FLINK_CHECKPOINT_INTERVAL_SECONDS)" \
		-e FLINK_CHECKPOINT_MIN_PAUSE="$(FLINK_CHECKPOINT_MIN_PAUSE)" \
		-e FLINK_MINI_BATCH_INTERVAL="$(FLINK_MINI_BATCH_INTERVAL)" \
		-e FLINK_MINI_BATCH_SIZE="$(FLINK_MINI_BATCH_SIZE)" \
		-e GOLD_ONLY=$(GOLD_ONLY) \
		$(C_FLINK) /opt/flink/bin/flink run -d -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3 -py /opt/flink/work-dir/flink_gold.py

# =============================================================================
# JOB CONTROL (streaming jobs only — submitted detached)
# =============================================================================

list-jobs:
	docker exec $(C_FLINK) /opt/flink/bin/flink list

# Cancel a specific streaming job by name substring match.
# Usage:  make cancel-silver  (cancels any RUNNING job whose name contains "silver")
cancel-bronze:
	@$(MAKE) _cancel-by-name NAME=bronze

cancel-silver:
	@$(MAKE) _cancel-by-name NAME=silve

cancel-gold:
	@$(MAKE) _cancel-by-name NAME=gold

cancel-all:
	@docker exec $(C_FLINK) /opt/flink/bin/flink list 2>/dev/null \
		| awk '/RUNNING/ {print $$4}' \
		| xargs -r -I{} docker exec $(C_FLINK) /opt/flink/bin/flink cancel {}

# Internal: cancel RUNNING jobs whose flink-list line contains $(NAME).
# Flink's `flink list` line format: "<date> : <jobid> : <jobname> (RUNNING)"
_cancel-by-name:
	@docker exec $(C_FLINK) /opt/flink/bin/flink list 2>/dev/null \
		| awk -v n="$(NAME)" 'tolower($$0) ~ tolower(n) && /RUNNING/ {print $$4}' \
		| xargs -r -I{} docker exec $(C_FLINK) /opt/flink/bin/flink cancel {} \
		|| echo "[cancel] no RUNNING job matching '$(NAME)'"

# Export — batch: Gold Iceberg → PostgreSQL sink (Grafana)
export-gold:
	docker exec \
		-e ICEBERG_CATALOG_URI=$(ICEBERG_CATALOG_URI) \
		-e ICEBERG_WAREHOUSE=$(ICEBERG_WAREHOUSE) \
		-e PG_SINK_HOST=postgres-sink \
		-e PG_SINK_PORT=5432 \
		-e PG_SINK_DB=${PG_SINK_DB} \
		-e PG_SINK_USER=${PG_SINK_USER} \
		-e PG_SINK_PASSWORD=${PG_SINK_PASSWORD} \
		$(C_FLINK) /opt/flink/bin/flink run -pyexec /usr/bin/python3 -pyclientexec /usr/bin/python3 -py /opt/flink/work-dir/flink_gold_to_postgres.py

# =============================================================================
# CHECKS
# =============================================================================

check-kafka:
	docker exec $(C_KAFKA) bash -lc \
		'cd /opt/kafka/bin && ./kafka-topics.sh --bootstrap-server localhost:19092 --list'

check-iceberg:
	@curl -sf http://localhost:8181/v1/namespaces | python3 -m json.tool
	@echo "--- bronze tables ---"
	@curl -sf "http://localhost:8181/v1/namespaces/bronze/tables" | python3 -m json.tool || true
	@echo "--- silver tables ---"
	@curl -sf "http://localhost:8181/v1/namespaces/silver/tables" | python3 -m json.tool || true
	@echo "--- gold tables ---"
	@curl -sf "http://localhost:8181/v1/namespaces/gold/tables" | python3 -m json.tool || true

check-silver:
	@curl -sf "http://localhost:8181/v1/namespaces/silver/tables" \
		| python3 -c "import sys,json; t=json.load(sys.stdin).get('identifiers',[]); \
		  print(f'[silver] {len(t)} table(s): {[x[\"name\"] for x in t]}')"

check-gold:
	@curl -sf "http://localhost:8181/v1/namespaces/gold/tables" \
		| python3 -c "import sys,json; t=json.load(sys.stdin).get('identifiers',[]); \
		  print(f'[gold] {len(t)} table(s): {[x[\"name\"] for x in t]}')"

check-mlflow:
	@curl -sf http://localhost:5000/health && echo "[mlflow] OK" || echo "[mlflow] UNREACHABLE"

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

train-ai:
	@curl -sf -X POST http://localhost:8000/train | python3 -m json.tool

build-flink:
	docker compose build flink-jobmanager flink-taskmanager

# =============================================================================
# RESET
# =============================================================================

reset:
	docker exec $(C_KAFKA) bash -lc '\
		cd /opt/kafka/bin && \
		for t in $$(./kafka-topics.sh --bootstrap-server localhost:19092 --list); do \
			./kafka-topics.sh --bootstrap-server localhost:19092 --delete --if-exists --topic "$$t"; \
		done'
	@echo "[reset] dropping Iceberg tables via REST catalog..."
	@for ns in bronze silver gold; do \
		tables=$$(curl -sf "http://localhost:8181/v1/namespaces/$$ns/tables" \
			| python3 -c "import sys,json; [print(x['name']) for x in json.load(sys.stdin).get('identifiers',[])]" 2>/dev/null); \
		for tbl in $$tables; do \
			curl -sf -X DELETE "http://localhost:8181/v1/namespaces/$$ns/tables/$$tbl" && \
				echo "[reset] dropped $$ns.$$tbl" || true; \
		done; \
	done
	@echo "[reset] clearing HDFS Iceberg warehouse and Flink checkpoints..."
	-docker exec $(C_NAMENODE) hdfs dfs -rm -r -skipTrash /warehouse/iceberg/* /checkpoints/flink/* >/dev/null 2>&1 || true
	@$(MAKE) init-hdfs

reset-silver:
	@tables=$$(curl -sf "http://localhost:8181/v1/namespaces/silver/tables" \
		| python3 -c "import sys,json; [print(x['name']) for x in json.load(sys.stdin).get('identifiers',[])]" 2>/dev/null); \
	for tbl in $$tables; do \
		curl -sf -X DELETE "http://localhost:8181/v1/namespaces/silver/tables/$$tbl" && \
			echo "[reset] dropped silver.$$tbl" || true; \
	done
	@echo "[reset] Silver cleared — run: make silver"

reset-gold:
	@tables=$$(curl -sf "http://localhost:8181/v1/namespaces/gold/tables" \
		| python3 -c "import sys,json; [print(x['name']) for x in json.load(sys.stdin).get('identifiers',[])]" 2>/dev/null); \
	for tbl in $$tables; do \
		curl -sf -X DELETE "http://localhost:8181/v1/namespaces/gold/tables/$$tbl" && \
			echo "[reset] dropped gold.$$tbl" || true; \
	done
	@echo "[reset] Gold cleared — run: make gold"

# =============================================================================
# BENCHMARK
# =============================================================================

benchmark:
	REQUEST_RATES=$(REQUEST_RATES) \
	N_RUNS=$(N_RUNS) \
	WARMUP_RUNS=$(WARMUP_RUNS) \
	WARMUP_SECS=$(WARMUP_SECS) \
	PARALLELISM=$(PARALLELISM) \
	KAFKA_PARTITIONS=$(KAFKA_PARTITIONS) \
	ICEBERG_MONITOR_INTERVAL="$(ICEBERG_MONITOR_INTERVAL)" \
	FLINK_CHECKPOINT_INTERVAL="$(FLINK_CHECKPOINT_INTERVAL)" \
	FLINK_CHECKPOINT_INTERVAL_SECONDS="$(FLINK_CHECKPOINT_INTERVAL_SECONDS)" \
	FLINK_CHECKPOINT_MIN_PAUSE="$(FLINK_CHECKPOINT_MIN_PAUSE)" \
	FLINK_MINI_BATCH_INTERVAL="$(FLINK_MINI_BATCH_INTERVAL)" \
	FLINK_MINI_BATCH_SIZE="$(FLINK_MINI_BATCH_SIZE)" \
	GOLD_CHECKPOINT_SECONDS=$(GOLD_CHECKPOINT_SECONDS) \
	bash benchmark/run_benchmark.sh

benchmark-quick:
	REQUEST_RATES=5 N_RUNS=1 WARMUP_RUNS=0 WARMUP_SECS=10 PARALLELISM=$(PARALLELISM) KAFKA_PARTITIONS=$(KAFKA_PARTITIONS) bash benchmark/run_benchmark.sh
