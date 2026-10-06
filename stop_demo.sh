#!/usr/bin/env bash
# Stop the market simulator and Streamlit dashboard.
pkill -f "run_simulator" 2>/dev/null && echo "Simulator stopped." || echo "Simulator was not running."
pkill -f "streamlit run"  2>/dev/null && echo "Dashboard stopped."  || echo "Dashboard was not running."

echo ""
echo "======================================================"
echo "  ⚠  Don't forget to stop the Flink job in Confluent Cloud"
echo "     to avoid burning compute credits!"
echo ""
echo "  STOP Flink job:"
echo "    1. https://confluent.cloud  → sign in"
echo "    2. Environments → your environment → Flink"
echo "    3. Statements tab"
echo "    4. Find the running INSERT INTO market_signals statement"
echo "    5. Click ⋮ (menu) → Stop"
echo "======================================================"
