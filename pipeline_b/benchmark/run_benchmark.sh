#!/usr/bin/env bash
# =============================================================================
# Pipeline B — benchmark runner (Flink + Iceberg)
# Run from the pipeline_b/ directory: bash benchmark/run_benchmark.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  Pipeline B Benchmark (Flink + Iceberg)"
echo "  $(date)"
echo "============================================================"

# ── 1. Verify required containers are running ────────────────────────────────
echo "[check] Verifying required containers..."
for svc in flink-jobmanager flink-taskmanager kafka iceberg-rest minio; do
  id=$(docker compose ps -q "$svc" 2>/dev/null || true)
  if [ -z "$id" ]; then
    echo "[ERROR] $svc is not running. Run: docker compose up -d"
    exit 1
  fi
  echo "  ✓ $svc"
done

# ── 2. Verify CSV files exist ────────────────────────────────────────────────
echo "[check] Verifying CSV data files..."
for f in hourly_fitbit_sema_df_unprocessed.csv daily_fitbit_sema_df_unprocessed.csv; do
  if [ ! -f "../data/$f" ]; then
    echo "[ERROR] Missing: ../data/$f"
    echo "        Put the shared CSV files under the repository-level data/ directory."
    exit 1
  fi
  echo "  ✓ $f"
done

# ── 3. Verify Kafka topics are initialised ───────────────────────────────────
echo "[check] Verifying Kafka topics..."
C_KAFKA=$(docker compose ps -q kafka)
echo "[init] Ensuring Kafka topics exist with ${KAFKA_PARTITIONS:-12} partition(s)..."
make init-kafka
topic_count=$(docker exec "$C_KAFKA" bash -lc \
  'cd /opt/kafka/bin && ./kafka-topics.sh --bootstrap-server localhost:19092 --list 2>/dev/null | grep -c wearable || true')
echo "  ✓ $topic_count Kafka topics"

# ── 4. Verify Iceberg namespaces ─────────────────────────────────────────────
echo "[check] Verifying Iceberg namespaces..."
ns_count=$(curl -sf "http://localhost:8181/v1/namespaces" \
  | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('namespaces',[])))" 2>/dev/null || echo 0)
if [ "$ns_count" -lt 3 ]; then
  echo "[init] Iceberg namespaces missing — running make init-iceberg..."
  make init-iceberg
fi
echo "  ✓ Iceberg namespaces ready"

# ── 5. Pick Python ───────────────────────────────────────────────────────────
if [ -f ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="python3"
fi
$PYTHON -c "import requests" 2>/dev/null || {
  echo "[install] Installing requests..."
  $PYTHON -m pip install requests -q
}

# ── 5b. Ensure all 3 STREAMING Flink jobs are running ───────────────────────
# Pipeline B is full streaming now: Bronze + Silver + Gold are continuous
# detached Flink jobs. We submit them once, then they run for the whole
# benchmark. benchmark.py refuses to start unless all 3 are RUNNING.
C_FLINK=$(docker compose ps -q flink-jobmanager)

count_running_matching() {
  docker exec "$C_FLINK" /opt/flink/bin/flink list 2>/dev/null \
    | awk -v n="$1" 'tolower($0) ~ tolower(n) && /RUNNING/' \
    | wc -l
}

echo "[check] Streaming Flink jobs (bronze / silver / gold)…"
bronze_n=$(count_running_matching bronze)
silver_n=$(count_running_matching silver)
gold_n=$(count_running_matching gold)
echo "  running: bronze=$bronze_n silver=$silver_n gold=$gold_n"

# Submit Bronze first. The measured workload comes from producer_realtime.py.
if [ "$bronze_n" -eq 0 ]; then
  echo "  → Bronze not running. Submitting bronze…"
  make bronze
  echo "  ✓ bronze submitted (detached)"
  sleep 15   # give Bronze a chance to register sources before Silver starts reading
fi

# If Silver is not already running, clear Silver tables so the benchmark starts`r`n# from the current realtime-only schema.
if [ "$silver_n" -eq 0 ]; then
  echo "  → Silver not running. Clearing silver.* tables then submitting…"
  make reset-silver || true
  make silver
  echo "  ✓ silver submitted (detached)"
fi

# If Gold is not already running, clear Gold tables so the benchmark starts from`r`n# the current realtime-only schema.
if [ "$gold_n" -eq 0 ]; then
  echo "  → Gold not running. Clearing gold.* tables then submitting…"
  make reset-gold || true
  make gold
  echo "  ✓ gold submitted (detached)"
fi

# Only sleep if we just submitted new jobs — already-running jobs don't need
# a warm-up wait (they've been running continuously).
if [ "$bronze_n" -eq 0 ] || [ "$silver_n" -eq 0 ] || [ "$gold_n" -eq 0 ]; then
  echo "[wait] Newly submitted jobs — waiting 90s for first checkpoint …"
  sleep 90
else
  echo "[check] Jobs already running — skipping warm-up wait"
fi

# Re-verify all 3 are running before handing off to benchmark.py
bronze_n=$(count_running_matching bronze)
silver_n=$(count_running_matching silver)
gold_n=$(count_running_matching gold)
echo "  final: bronze=$bronze_n silver=$silver_n gold=$gold_n"
if [ "$bronze_n" -lt 1 ] || [ "$silver_n" -lt 1 ] || [ "$gold_n" -lt 1 ]; then
  echo "[ERROR] One or more streaming jobs failed to start. See Flink UI / logs."
  exit 1
fi

# ── 6a. Source .env so benchmark.py picks up MINIO / PG_SINK / etc. creds ────
if [ -f ".env" ]; then
  echo "[config] Sourcing .env"
  set -a   # auto-export every variable defined below
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

echo "[config] Using local Makefile defaults. Override benchmark settings with env vars when needed."

# ── 7. Run benchmark ─────────────────────────────────────────────────────────
echo ""
echo "[benchmark] Starting..."
echo "  REQUEST_RATES : ${REQUEST_RATES:-50,100,200}"
echo "  N_RUNS        : ${N_RUNS:-3}"
echo "  WARMUP_RUNS   : ${WARMUP_RUNS:-1}"
echo "  WARMUP_SECS   : ${WARMUP_SECS:-10}"
echo "  KAFKA_PARTITIONS: ${KAFKA_PARTITIONS:-12}"
echo "  PARALLELISM   : ${PARALLELISM:-6}"
echo "  ICEBERG_MONITOR_INTERVAL: ${ICEBERG_MONITOR_INTERVAL:-15s}"
echo "  FLINK_CHECKPOINT_INTERVAL: ${FLINK_CHECKPOINT_INTERVAL:-15 s}"
echo ""

LOG="benchmark/benchmark_$(date +%Y%m%d_%H%M%S).log"

REQUEST_RATES="${REQUEST_RATES:-50,100,200}" \
N_RUNS="${N_RUNS:-3}" \
WARMUP_RUNS="${WARMUP_RUNS:-1}" \
WARMUP_SECS="${WARMUP_SECS:-10}" \
DELAY="${DELAY:-0.1}" \
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-12}" \
PARALLELISM="${PARALLELISM:-6}" \
ICEBERG_MONITOR_INTERVAL="${ICEBERG_MONITOR_INTERVAL:-15s}" \
FLINK_CHECKPOINT_INTERVAL="${FLINK_CHECKPOINT_INTERVAL:-15 s}" \
FLINK_MINI_BATCH_INTERVAL="${FLINK_MINI_BATCH_INTERVAL:-5 s}" \
FLINK_MINI_BATCH_SIZE="${FLINK_MINI_BATCH_SIZE:-5000}" \
$PYTHON benchmark/benchmark.py 2>&1 | tee "$LOG"

echo ""
echo "============================================================"
echo "  Done! Results in benchmark/   Log: $LOG"
echo "============================================================"
