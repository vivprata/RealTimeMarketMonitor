#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_demo.sh — start the market simulator + Streamlit dashboard.
#
# Usage:
#   ./run_demo.sh               # random background shocks ON
#   ./run_demo.sh --no-random   # button-only shocks (manual IBM trigger)
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/.venv/bin"

# Kill any lingering copies so we start clean.
pkill -f "run_simulator" 2>/dev/null || true
pkill -f "streamlit run" 2>/dev/null || true
sleep 1

RANDOM_FLAG="--random-shocks"
if [[ "${1:-}" == "--no-random" ]]; then
  RANDOM_FLAG=""
  echo "[demo] Starting in button-only shock mode."
fi

echo "[demo] Starting market simulator (warmup 10 min + seed portfolio)..."
cd "$SCRIPT_DIR"
"$VENV/python" -m simulator.run_simulator \
  --seed-portfolio \
  --warmup 10 \
  $RANDOM_FLAG \
  --quiet &
SIM_PID=$!
echo "[demo] Simulator PID: $SIM_PID"

echo "[demo] Starting Streamlit dashboard..."
"$VENV/streamlit" run dashboard/Live_Market_Monitor.py \
  --server.address 127.0.0.1 \
  --server.port 8501 &
DASH_PID=$!
echo "[demo] Dashboard PID: $DASH_PID"

echo ""
echo "======================================================"
echo "  Demo running at http://localhost:8501"
echo "  Simulator PID : $SIM_PID"
echo "  Dashboard PID : $DASH_PID"
echo "======================================================"
echo "  Stop with: ./stop_demo.sh"
echo ""
