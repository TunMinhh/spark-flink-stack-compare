#!/usr/bin/env bash
# =============================================================================
# Pipeline A - benchmark runner (Spark Structured Streaming + Delta Lake)
# Run from the pipeline_a/ directory: bash benchmark/run_benchmark.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  Pipeline A Benchmark (Spark Structured Streaming + Delta)"
echo "  $(date)"
echo "============================================================"

echo "[check] Verifying required containers..."
for svc in spark-master spark-worker namenode datanode kafka; do
  id=$(docker compose ps -q "$svc" 2>/dev/null || true)
  if [ -z "$id" ]; then
    echo "[ERROR] $svc container not running. Run: docker compose up -d"
    exit 1
  fi
  echo "  OK $svc"
done

echo "[init] Ensuring HDFS paths and Kafka topics exist..."
make init

start_stream_if_missing() {
  local script="$1"
  local target="$2"
  local log="$3"
  local wait_secs="$4"

  if ! pgrep -f "$script" > /dev/null 2>&1; then
    echo "  -> $target not running; starting make $target in the background..."
    nohup make "$target" \
      BRONZE_TRIGGER_SECONDS="${BRONZE_TRIGGER_SECONDS:-15}" \
      SILVER_TRIGGER_SECONDS="${SILVER_TRIGGER_SECONDS:-15}" \
      GOLD_TRIGGER_SECONDS="${GOLD_TRIGGER_SECONDS:-15}" \
      BRONZE_CORES="${BRONZE_CORES:-6}" \
      SILVER_CORES="${SILVER_CORES:-6}" \
      GOLD_CORES="${GOLD_CORES:-6}" \
      SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-18}" \
      SILVER_WRITE_COALESCE="${SILVER_WRITE_COALESCE:-2}" \
      SPARK_MAX_RECORDS_PER_FILE="${SPARK_MAX_RECORDS_PER_FILE:-50000}" \
      > "$log" 2>&1 &
    echo "$!" > "/tmp/pipeline_a_${target}.pid"
    echo "     [$target] streaming started (PID $(cat /tmp/pipeline_a_${target}.pid))"
    sleep "$wait_secs"
  else
    echo "  OK $target already running"
  fi
}

echo "[check] Spark Structured Streaming jobs (bronze / silver / gold)..."
start_stream_if_missing "spark_bronze.py" "bronze" "/tmp/pipeline_a_bronze.log" 30
start_stream_if_missing "spark_silver_streaming.py" "silver" "/tmp/pipeline_a_silver.log" 45
start_stream_if_missing "spark_gold_streaming.py" "gold" "/tmp/pipeline_a_gold.log" 45

if [ -f ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi

echo "[config] Using local Makefile defaults. Override benchmark settings with env vars when needed."

echo ""
echo "[benchmark] Starting..."
echo "  REQUEST_RATES     : ${REQUEST_RATES:-50,100,200}"
echo "  N_RUNS            : ${N_RUNS:-3}"
echo "  WARMUP_RUNS       : ${WARMUP_RUNS:-1}"
echo "  WARMUP_SECS       : ${WARMUP_SECS:-10}"
echo "  KAFKA_PARTITIONS  : ${KAFKA_PARTITIONS:-12}"
echo "  SHUFFLE_PARTITIONS: ${SHUFFLE_PARTITIONS:-18}"
echo "  TRIGGER_SECONDS   : bronze=${BRONZE_TRIGGER_SECONDS:-15} silver=${SILVER_TRIGGER_SECONDS:-15} gold=${GOLD_TRIGGER_SECONDS:-15}"
echo ""

LOG="benchmark/benchmark_$(date +%Y%m%d_%H%M%S).log"

REQUEST_RATES="${REQUEST_RATES:-50,100,200}" \
N_RUNS="${N_RUNS:-3}" \
WARMUP_RUNS="${WARMUP_RUNS:-1}" \
WARMUP_SECS="${WARMUP_SECS:-10}" \
DELAY="${DELAY:-0.1}" \
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-12}" \
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-18}" \
"$PYTHON" benchmark/benchmark.py 2>&1 | tee "$LOG"

echo ""
echo "============================================================"
echo "  Done! Results in benchmark/   Log: $LOG"
echo "============================================================"
