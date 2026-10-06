"""
scripts/inject_anomaly.py — Switch the running demo simulator between
'all_clear' and 'anomaly' sub-modes mid-demo without restarting it.

HOW IT WORKS
------------
The simulator's DEMO scenario watches the DEMO_SCENARIO_FILE sentinel file.
This script writes the chosen sub-mode to that file.  The running simulator
polls for changes every tick and calls engine.set_demo_submode() when it sees
a new value.  Re-anchoring happens immediately; Flink's 5-minute window will
show the new move_pct after one commit cycle (~30–60 s via Tableflow).

USAGE
-----
# Switch to anomaly mode (MSFT ~5σ, TSLA ~3σ):
    python scripts/inject_anomaly.py anomaly

# Switch back to all-clear (all symbols < 1.5σ):
    python scripts/inject_anomaly.py all_clear

# Show current mode:
    python scripts/inject_anomaly.py status

DEMO FLOW
---------
1. Start the simulator in DEMO mode:
       ./run_demo.sh --demo          # or:
       .venv/bin/python -m simulator.run_simulator \\
           --scenario demo --demo-scenario all_clear \\
           --warmup 10 --seed-portfolio --quiet

2. Open the Anomaly Detection page — ALL CLEAR banner shows.

3. ~1 minute before the reveal run:
       python scripts/inject_anomaly.py anomaly

4. Wait ~30–60 s for Tableflow to commit the new Iceberg snapshot.

5. Click "Refresh now" on the Anomaly Detection page.
   MSFT 🚨 RISK ALERT banner fires (~5σ).

6. After the demo, reset:
       python scripts/inject_anomaly.py all_clear
"""

from __future__ import annotations

import sys
from pathlib import Path

# The sentinel file the simulator watches.  Lives next to the project root so
# it is gitignored (see .gitignore: data/).
_ROOT = Path(__file__).resolve().parent.parent
SENTINEL_FILE = _ROOT / "data" / "demo_scenario.txt"

VALID_MODES = {"anomaly", "all_clear"}


def _read_current() -> str:
    if SENTINEL_FILE.exists():
        return SENTINEL_FILE.read_text().strip()
    return "(not set — simulator using DEMO_SCENARIO env var or default 'anomaly')"


def _write(mode: str) -> None:
    SENTINEL_FILE.parent.mkdir(parents=True, exist_ok=True)
    SENTINEL_FILE.write_text(mode)


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1].strip().lower()

    if cmd == "status":
        print(f"Current demo scenario: {_read_current()}")
        return

    if cmd not in VALID_MODES:
        print(
            f"Error: unknown mode '{cmd}'. Choose: {' | '.join(sorted(VALID_MODES))}",
            file=sys.stderr,
        )
        sys.exit(1)

    _write(cmd)
    print(f"✓ Demo scenario set to '{cmd}'.")

    if cmd == "anomaly":
        print(
            "  MSFT → -6.40% (~5.1σ) 🚨  TSLA → -10.40% (~3.1σ) 🚨\n"
            "  Wait ~30–60 s for Tableflow commit, then refresh Anomaly Detection."
        )
    else:
        print(
            "  All symbols → < 1.5σ  ✅\n"
            "  Wait ~30–60 s for Tableflow commit, then refresh Anomaly Detection."
        )


if __name__ == "__main__":
    main()
