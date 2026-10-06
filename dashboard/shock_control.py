"""Helper to trigger the market shock by signaling the running simulator process."""

from __future__ import annotations

import os
import signal
import subprocess


def find_simulator_pids() -> list[int]:
    """Return PIDs of running `run_simulator` processes."""
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "run_simulator"], text=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []
    pids = []
    for line in out.split():
        try:
            pid = int(line)
            if pid != os.getpid():
                pids.append(pid)
        except ValueError:
            continue
    return pids


def trigger_shock() -> tuple[bool, str]:
    """Send SIGUSR1 to the simulator. Returns (success, message)."""
    pids = find_simulator_pids()
    if not pids:
        return False, "No running simulator found — start run_simulator first."
    for pid in pids:
        try:
            os.kill(pid, signal.SIGUSR1)
        except ProcessLookupError:
            continue
    return (
        True,
        f"Market shock injected (signaled PID(s): {', '.join(map(str, pids))}). "
        "Wait ~60-90s, then open Anomaly Detection to see the RISK ALERT.",
    )
