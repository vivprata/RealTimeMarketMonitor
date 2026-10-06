"""
Streamlit dashboard — Page 1: Live Market (main page).

Sections:
  1. Portfolio Summary   value, today's P&L, holdings, events/sec
  2. Live Market         per-symbol table: price, change, volume, status
  3. Live Price Chart    continuously updating line chart for a selected symbol
  4. Streaming Events    most recent ticks (makes Kafka visibly "live")

Plus an INJECT MARKET SHOCK button that signals the running simulator.

Real-time strategy: a background thread (dashboard/market_data.py) drains Kafka
into shared state; this page reads that state and auto-reruns on a 1s timer.
Streamlit never blocks on Kafka.

Run:
    .venv/bin/streamlit run dashboard/Live_Market_Monitor.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

# Streamlit runs this file as a script; add the project root so absolute imports resolve.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.market_data import start_consumer
from dashboard.shock_control import trigger_shock
from dashboard.theme import (
    flow_strip,
    header,
    inject_css,
    live_badge,
    metric_card,
    status_pill,
)

REFRESH_SECONDS = 1.0


# --- status classification (mirrors the Flink rules) ---
def classify(change_pct: float, vol_ratio: float) -> str:
    if change_pct < -3 and vol_ratio > 2:
        return "HIGH_VOLUME_DROP"
    if change_pct <= -5:
        return "SIGNIFICANT_DROP"
    if change_pct <= -3:
        return "DOWN"
    if change_pct >= 3:
        return "UP"
    return "NORMAL"


@st.cache_resource
def _state():
    """Start the background Kafka consumer once per server process."""
    return start_consumer()


def _fmt_vol(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{int(v)}"


def main() -> None:
    st.set_page_config(page_title="Live Market", page_icon="📈", layout="wide")
    inject_css()
    state = _state()
    snap = state.snapshot()
    rows = snap["rows"]

    # ---- Sidebar: controls ----
    with st.sidebar:
        st.markdown("### ⚡ Demo Controls")
        st.markdown(
            "Inject a sudden market event and watch it flow through the whole "
            "pipeline — then see it scored as an anomaly on the "
            "**Anomaly Detection** page."
        )
        if st.button("🔴  INJECT MARKET SHOCK", type="primary", use_container_width=True):
            ok, msg = trigger_shock()
            (st.success if ok else st.error)(msg)
        st.caption("Signals the running simulator (SIGUSR1).")
        st.divider()
        auto = st.toggle("Auto-refresh", value=True)
        st.caption(f"Live updates every {REFRESH_SECONDS:.0f}s.")
        st.divider()
        st.markdown(
            live_badge(f"{snap['events_per_sec']:.0f} events/sec streaming"),
            unsafe_allow_html=True,
        )

    header(
        "Real-Time Market Monitoring & Risk Surveillance",
        "Confluent (Kafka · Flink · Tableflow) → Apache Iceberg → "
        "IBM watsonx.data → Streamlit",
    )
    flow_strip(active={"Kafka", "Flink"})

    # Wait until the consumer has drained its historical backlog before showing
    # values — otherwise the UI flickers through stale replayed signals.
    if not state.is_ready():
        st.info("⏳ Connecting to the live stream and loading current state…")
        time.sleep(0.8)
        st.rerun()
        return

    if not rows:
        st.info(
            "Waiting for market ticks… start the simulator:\n\n"
            "`.venv/bin/python -m simulator.run_simulator --seed-portfolio --warmup 10`"
        )
        if auto:
            time.sleep(REFRESH_SECONDS)
            st.rerun()
        return

    # Enrich rows: prefer Flink's authoritative signal; fall back to local calc.
    for r in rows:
        if r.get("flink_status") is not None:
            r["status"] = r["flink_status"]
            r["disp_change"] = r["flink_change_pct"]
            r["disp_vol_ratio"] = r["flink_vol_ratio"]
            r["source"] = "flink"
        else:
            r["status"] = classify(r["change_pct"], r["vol_ratio"])
            r["disp_change"] = r["change_pct"]
            r["disp_vol_ratio"] = r["vol_ratio"]
            r["source"] = "local"
    by_symbol = {r["symbol"]: r for r in rows}
    flink_live = any(r["source"] == "flink" for r in rows)

    # ---- Section 1: Portfolio Summary ----
    portfolio = snap["portfolio"]
    total_value = 0.0
    total_cost = 0.0
    for sym, pos in portfolio.items():
        r = by_symbol.get(sym)
        if not r:
            continue
        shares = pos.get("shares", 0)
        total_value += shares * r["price"]
        total_cost += shares * pos.get("purchase_price", 0.0)
    pnl = total_value - total_cost
    pnl_pct = (pnl / total_cost * 100) if total_cost else 0.0

    alerts = [
        r for r in rows
        if r["status"] in ("DOWN", "SIGNIFICANT_DROP", "HIGH_VOLUME_DROP")
    ]

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        metric_card("Portfolio Value", f"${total_value:,.0f}" if total_value else "—"),
        unsafe_allow_html=True,
    )
    c2.markdown(
        metric_card(
            "Unrealized P&L",
            f"${pnl:,.0f}" if total_value else "—",
            delta=f"{pnl_pct:+.2f}%" if total_cost else None,
            delta_positive=pnl >= 0,
        ),
        unsafe_allow_html=True,
    )
    c3.markdown(
        metric_card(
            "Holdings",
            str(len(portfolio) or len(rows)),
            sub="fictional demo portfolio",
        ),
        unsafe_allow_html=True,
    )
    c4.markdown(
        metric_card(
            "Events / sec",
            f"{snap['events_per_sec']:.1f}",
            sub="live Kafka throughput",
        ),
        unsafe_allow_html=True,
    )

    if alerts:
        worst = min(alerts, key=lambda r: r["disp_change"] or 0)
        st.markdown(
            f"<div style='margin-top:10px;padding:10px 14px;border-radius:10px;"
            f"background:#fef2f2;border:1px solid #fecaca;color:#7f1d1d;font-weight:600'>"
            f"⚠ {len(alerts)} symbol(s) moving sharply — "
            f"{worst['symbol']} {worst['disp_change']:+.2f}% {status_pill(worst['status'])}"
            f" &nbsp;→ open <b>Anomaly Detection</b> to see if it's a real anomaly "
            f"vs its history.</div>",
            unsafe_allow_html=True,
        )

    st.caption(
        "🟢 Status computed by **Flink** from `market_signals` (live)"
        if flink_live else
        "Status: local approximation (Flink signals not received yet)"
    )

    st.divider()

    left, right = st.columns([3, 2])

    # ---- Section 2: Live Market table ----
    with left:
        st.markdown("#### 📊 Live Market")
        df = pd.DataFrame(
            [
                {
                    "Symbol": r["symbol"],
                    "Price": r["price"],
                    "Change": r["disp_change"] if r["disp_change"] is not None else 0.0,
                    "Volume": _fmt_vol(r["volume"]),
                    "Vol Ratio": (r["disp_vol_ratio"] or 0),
                    "Status": r["status"],
                }
                for r in sorted(rows, key=lambda x: (x["disp_change"] or 0))
            ]
        )
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "Change": st.column_config.NumberColumn(format="%+.2f%%"),
                "Vol Ratio": st.column_config.NumberColumn(format="%.2fx"),
            },
        )

    # ---- Section 4: Streaming Events ----
    with right:
        st.markdown("#### 🟢 Streaming Events")
        events = snap["events"][:14]
        if events:
            ev_df = pd.DataFrame(
                [
                    {
                        "Time": ts[11:19] if len(ts) > 19 else ts,
                        "Symbol": sym,
                        "Price": price,
                    }
                    for ts, sym, price in events
                ]
            )
            st.dataframe(
                ev_df,
                use_container_width=True,
                hide_index=True,
                height=340,
                column_config={
                    "Price": st.column_config.NumberColumn(format="$%.2f")
                },
            )
        else:
            st.caption("No events yet.")

    st.divider()

    # ---- Section 3: Live Price Chart ----
    st.markdown("#### 📈 Live Price Chart")
    symbols = sorted(by_symbol.keys())
    default_ix = symbols.index("IBM") if "IBM" in symbols else 0
    sel = st.selectbox("Symbol", symbols, index=default_ix, label_visibility="collapsed")

    sr = by_symbol[sel]
    down = sr["status"] in ("DOWN", "SIGNIFICANT_DROP", "HIGH_VOLUME_DROP")
    f1, f2, f3, f4 = st.columns(4)
    f1.markdown(metric_card("Price", f"${sr['price']:,.2f}"), unsafe_allow_html=True)
    f2.markdown(
        metric_card(
            "Change",
            f"{(sr['disp_change'] or 0):+.2f}%",
            sub="vs 5-min baseline (Flink)",
        ),
        unsafe_allow_html=True,
    )
    f3.markdown(
        metric_card(
            "Volume Ratio",
            f"{(sr['disp_vol_ratio'] or 0):.2f}×",
            sub="vs 30s baseline",
        ),
        unsafe_allow_html=True,
    )
    f4.markdown(
        f'<div class="mcard"><div class="label">Status</div>'
        f'<div style="margin-top:8px">{status_pill(sr["status"])}</div>'
        f'<div class="sub">from Flink market_signals</div></div>',
        unsafe_allow_html=True,
    )

    hist = state.history_for(sel)
    if len(hist) >= 2:
        chart_df = pd.DataFrame(hist, columns=["ts", "price"])
        chart_df["ts"] = pd.to_datetime(chart_df["ts"], errors="coerce")
        chart_df = chart_df.dropna().sort_values("ts")

        lo = float(chart_df["price"].min())
        hi = float(chart_df["price"].max())
        rng = hi - lo
        pad = rng * 0.12 if rng > 0.02 else max(hi * 0.0008, 0.05)
        y_domain = [lo - pad, hi + pad]
        pct_move = (rng / lo * 100) if lo else 0.0
        line_color = "#dc2626" if down else "#2563eb"

        base = alt.Chart(chart_df).encode(
            x=alt.X("ts:T", title=None, axis=alt.Axis(format="%H:%M:%S")),
            y=alt.Y(
                "price:Q",
                title="Price ($)",
                scale=alt.Scale(domain=y_domain, zero=False, nice=False),
                axis=alt.Axis(format="$.2f"),
            ),
        )
        line = base.mark_line(
            color=line_color, strokeWidth=2.5, interpolate="monotone"
        )
        pts = base.mark_circle(size=28, color=line_color).encode(
            tooltip=[
                alt.Tooltip("ts:T", title="Time", format="%H:%M:%S"),
                alt.Tooltip("price:Q", title="Price", format="$.2f"),
            ]
        )
        st.altair_chart(
            (line + pts).properties(height=300), use_container_width=True
        )
        st.caption(
            f"{sel}: ${lo:,.2f} – ${hi:,.2f} in view "
            f"(range {pct_move:.2f}%). Y-axis zoomed to data."
        )
    else:
        st.caption("Collecting price history…")

    if auto:
        time.sleep(REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
