"""
Phase 2 — Market price engine and scenarios (NO I/O).

This module is deliberately free of Kafka/AWS so it can be unit-tested and
dry-run without any cloud connection. The runner (run_simulator.py) wraps it
with a Confluent Cloud producer.

Design decisions:
  * Deterministic (seeded) random walk -> repeatable demos.
  * A `warmup` mode emits back-dated ticks at burst speed so Flink's rolling
    window is already populated before the live portion.
  * The market shock drives a controlled multi-step drop + volume spike
    engineered to cross Flink's HIGH_VOLUME_DROP threshold.
  * Multiple symbols can be shocked concurrently (e.g. manual IBM button plus
    a random background shock on another name).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

from config.settings import SYMBOLS, Symbol


class Scenario(str, Enum):
    NORMAL = "normal"
    RALLY = "rally"
    SELLOFF = "selloff"
    SHOCK = "shock"


# Per-scenario drift/vol multipliers applied to each tick's return.
SCENARIO_PROFILE = {
    Scenario.NORMAL:  {"drift": 0.0,      "vol_mult": 1.0, "volume_mult": 1.0},
    Scenario.RALLY:   {"drift": 0.00025,  "vol_mult": 1.4, "volume_mult": 1.6},
    Scenario.SELLOFF: {"drift": -0.00030, "vol_mult": 1.8, "volume_mult": 2.2},
    # SHOCK is handled specially for the shocked symbol; others behave ~normal.
    Scenario.SHOCK:   {"drift": 0.0,      "vol_mult": 1.0, "volume_mult": 1.0},
}

# Shock choreography (percentage-based, symbol-agnostic):
#   drop over ~DROP_TICKS to -DROP_PCT, hold for ~HOLD_TICKS (so the shocked
#   state survives a Tableflow commit and shows on the anomaly page), then
#   recover over ~RECOVER_TICKS back toward the pre-shock price.
SHOCK_DROP_PCT = 0.055      # ~-5.5% trough vs pre-shock price
SHOCK_DROP_TICKS = 12       # ticks to reach the trough (front-loaded ease-out)
SHOCK_HOLD_TICKS = 600      # hold near the trough (~4-5 min wall-clock)
SHOCK_RECOVER_TICKS = 60    # ticks to drift back to ~pre-shock
SHOCK_VOLUME_MULT = 8.0     # peak volume burst at shock onset

# MSFT permanent anomaly: hold MSFT at -6.3% below its warmup price indefinitely.
# MSFT annual_volatility=0.199 → daily_σ=1.25% → -6.3% = ~5.0σ
# Held permanently so the Anomaly Detection page always shows MSFT as an anomaly.
# Other symbols are unaffected and trade normally.
# The INJECT SHOCK button still works on top of this for IBM.
MSFT_ANOMALY_SYMBOL   = "MSFT"
MSFT_ANOMALY_DROP_PCT = 0.063          # -6.3% → 5.0σ
MSFT_ANOMALY_VOLUME   = 3.4            # 3.4× normal volume (HIGH_VOLUME_DROP)
MSFT_ANOMALY_HOLD     = 999_999_999    # effectively infinite — never recovers


@dataclass
class Tick:
    timestamp: str
    symbol: str
    price: float
    volume: int
    bid: float
    ask: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "bid": self.bid,
            "ask": self.ask,
        }


class PriceEngine:
    """Stateful per-symbol price/volume generator.

    One engine instance holds the current price for every symbol and advances
    them one tick at a time under the active scenario.
    """

    BASE_TICK_VOL = 0.0006  # base per-tick volatility at ~1 tick/sec

    def __init__(self, seed: int = 42, symbols: list[Symbol] | None = None):
        self.rng = random.Random(seed)
        self.symbols = symbols or SYMBOLS
        self.price: dict[str, float] = {s.symbol: s.base_price for s in self.symbols}
        self.base_volume: dict[str, int] = {
            s.symbol: s.avg_daily_volume // 780 or 500 for s in self.symbols
        }
        self._vol_by_symbol = {s.symbol: s.annual_volatility for s in self.symbols}
        self.scenario = Scenario.NORMAL
        # Concurrent shocks: symbol -> {step, anchor, drop, hold, recover}
        self._shocks: dict[str, dict] = {}

    @property
    def _shock_active(self) -> bool:
        return bool(self._shocks)

    # -- scenario control --------------------------------------------------
    def set_scenario(self, scenario: Scenario) -> None:
        """Set the market scenario. Moving away from SHOCK cancels all shocks.
        Setting to SHOCK preserves any existing shocks (e.g. MSFT permanent anomaly)."""
        self.scenario = scenario
        if scenario != Scenario.SHOCK:
            self._shocks.clear()
        # If setting TO SHOCK, preserve existing shocks (arm_msft_anomaly already set them).

    def arm_msft_anomaly(self) -> None:
        """Lock MSFT permanently at -6.3% below current price (~5σ).

        Called once after warmup completes so the 5-minute Flink window has a
        clean baseline. MSFT never recovers — it stays in the anomaly band for
        the entire demo session. All other symbols trade normally.
        The INJECT SHOCK button still works for IBM shocks on top of this.
        """
        anchor = self.price.get(MSFT_ANOMALY_SYMBOL, 490.0)
        self.scenario = Scenario.SHOCK
        self._shocks[MSFT_ANOMALY_SYMBOL] = {
            "step": SHOCK_DROP_TICKS,          # skip straight to hold phase
            "anchor": anchor,
            "drop": SHOCK_DROP_TICKS,
            "hold": MSFT_ANOMALY_HOLD,         # hold forever
            "recover": SHOCK_RECOVER_TICKS,
            "drop_pct": MSFT_ANOMALY_DROP_PCT, # override drop amount
            "vol_mult": MSFT_ANOMALY_VOLUME,   # override volume
        }

    def trigger_shock(self, symbol: str | None = None,
                      drop_ticks: int | None = None) -> str:
        """Begin a sustained shock on `symbol` (random if None).

        Repeatable and concurrent — multiple symbols can be shocked at the same
        time. Calling again on the same symbol restarts its shock.
        Returns the shocked symbol name.
        """
        if symbol is None:
            symbol = self.rng.choice([s.symbol for s in self.symbols])
        self.scenario = Scenario.SHOCK
        self._shocks[symbol] = {
            "step": 0,
            "anchor": self.price.get(symbol, 100.0),
            "drop": drop_ticks or SHOCK_DROP_TICKS,
            "hold": SHOCK_HOLD_TICKS,
            "recover": SHOCK_RECOVER_TICKS,
        }
        return symbol

    # -- core stepping -----------------------------------------------------
    def _next_price_normal(self, sym: Symbol, profile: dict) -> float:
        daily_vol = self._vol_by_symbol[sym.symbol]
        tick_vol = self.BASE_TICK_VOL * (daily_vol / 0.30) * profile["vol_mult"]
        r = self.rng.normalvariate(profile["drift"], tick_vol)
        return round(self.price[sym.symbol] * math.exp(r), 2)

    def _shock_factor(self, symbol: str) -> float:
        """Price multiplier vs anchor for a shocked symbol (drop -> hold -> recover).
        Advances the shock step; removes the shock once recovery completes.
        Respects per-shock 'drop_pct' override (used by arm_msft_anomaly)."""
        sh = self._shocks[symbol]
        step, drop = sh["step"], sh["drop"]
        hold_end = drop + sh["hold"]
        rec_end = hold_end + sh["recover"]
        drop_pct = sh.get("drop_pct", SHOCK_DROP_PCT)  # per-shock override

        if step < drop:
            lin = step / max(drop - 1, 1)
            frac = 1.0 - (1.0 - lin) ** 2
            factor = 1.0 - drop_pct * frac
        elif step < hold_end:
            factor = 1.0 - drop_pct
        elif step < rec_end:
            lin = (step - hold_end) / max(sh["recover"] - 1, 1)
            factor = (1.0 - drop_pct) + drop_pct * lin
        else:
            del self._shocks[symbol]   # shock complete
            return 1.0
        sh["step"] += 1
        return factor

    def _next_price_shock(self, symbol: str) -> float:
        anchor = self._shocks[symbol]["anchor"]
        factor = self._shock_factor(symbol)
        jitter = self.rng.normalvariate(0.0, anchor * 0.0004)
        return round(anchor * factor + jitter, 2)

    def _volume(self, sym: Symbol, profile: dict) -> int:
        base = self.base_volume[sym.symbol]
        mult = profile["volume_mult"]
        sh = self._shocks.get(sym.symbol)
        if sh is not None:
            # Use per-shock vol_mult if set (e.g. MSFT permanent anomaly),
            # otherwise decay from SHOCK_VOLUME_MULT over the hold period.
            if "vol_mult" in sh:
                mult = sh["vol_mult"]
            else:
                progress = sh["step"] / max(sh["drop"] + sh["hold"], 1)
                decay = math.exp(-progress / 0.35)
                mult = 1.3 + (SHOCK_VOLUME_MULT - 1.3) * decay
        noise = math.exp(self.rng.normalvariate(0.0, 0.30))
        return max(100, int(base * mult * noise))

    def next_tick(self, sym: Symbol, ts: datetime) -> Tick:
        profile = SCENARIO_PROFILE[self.scenario]
        if sym.symbol in self._shocks:
            price = self._next_price_shock(sym.symbol)
        else:
            price = self._next_price_normal(sym, profile)
        self.price[sym.symbol] = price

        volume = self._volume(sym, profile)
        spread = max(0.01, round(price * 0.0002, 2))
        return Tick(
            timestamp=ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            symbol=sym.symbol,
            price=price,
            volume=volume,
            bid=round(price - spread, 2),
            ask=round(price + spread, 2),
        )

    def step_all(self, ts: datetime) -> list[Tick]:
        """Advance every symbol by one tick at time `ts`."""
        return [self.next_tick(sym, ts) for sym in self.symbols]

    # -- warmup ------------------------------------------------------------
    def warmup_ticks(self, minutes: int, ticks_per_minute: int = 12,
                     start: datetime | None = None) -> list[Tick]:
        """Generate a burst of back-dated NORMAL ticks to pre-fill Flink's window.

        Timestamps are back-dated so a downstream 5-minute window sees a full
        history the instant the live stream begins.
        """
        prev_scenario = self.scenario
        self.set_scenario(Scenario.NORMAL)
        now = start or datetime.now(timezone.utc)
        total = minutes * ticks_per_minute
        interval = timedelta(seconds=60 / ticks_per_minute)
        ticks: list[Tick] = []
        for i in range(total, 0, -1):
            ts = now - interval * i
            ticks.extend(self.step_all(ts))
        self.scenario = prev_scenario
        return ticks
