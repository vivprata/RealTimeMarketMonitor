"""
Streamlit real-time data layer.

Solves the plan's risk ("Streamlit isn't real-time"): a plain blocking Kafka
consumer would freeze Streamlit's single-threaded rerun model. Instead we run
the consumer in a background daemon thread that continuously drains Kafka into
thread-safe shared state. Streamlit pages just READ this state and auto-rerun on
a timer, never blocking on Kafka.

The consumer thread is created once per Streamlit session and cached via
`@st.cache_resource`, so reruns reuse the same thread/state.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import timezone
from typing import Any

import certifi

# macOS python.org builds lack a linked CA bundle; point the SR client's HTTP
# layer at certifi so Schema Registry HTTPS calls verify correctly.
os.environ.setdefault("SSL_CERT_FILE", certifi.where())

from confluent_kafka import Consumer

from config.settings import KafkaConfig, SYMBOLS

MAX_HISTORY_PER_SYMBOL = 600   # last N ticks kept for the live chart
MAX_RECENT_EVENTS = 40         # rows in the "Streaming Events" feed


@dataclass
class SymbolState:
    price: float = 0.0
    prev_price: float = 0.0      # first price seen this session (for % change)
    open_price: float = 0.0
    volume: int = 0
    bid: float = 0.0
    ask: float = 0.0
    last_ts: str = ""
    # rolling (timestamp, price) for the live chart
    history: deque = field(default_factory=lambda: deque(maxlen=MAX_HISTORY_PER_SYMBOL))
    # rolling volume window for client-side volume ratio display
    vol_window: deque = field(default_factory=lambda: deque(maxlen=60))


class MarketState:
    """Thread-safe container the background consumer writes and Streamlit reads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.symbols: dict[str, SymbolState] = {s.symbol: SymbolState() for s in SYMBOLS}
        self.portfolio: dict[str, dict] = {}      # symbol -> position record
        self.signals: dict[str, dict] = {}        # symbol -> latest Flink signal
        self.recent_events: deque = deque(maxlen=MAX_RECENT_EVENTS)
        self.tick_count: int = 0
        self.signal_count: int = 0
        self.started_at: float = time.monotonic()
        self._recent_tick_times: deque = deque(maxlen=200)
        self._ready = threading.Event()  # set once historical backlog is drained

    def mark_ready(self) -> None:
        self._ready.set()

    def is_ready(self) -> bool:
        return self._ready.is_set()

    # -- writers (called from consumer thread) ---------------------------
    def apply_tick(self, rec: dict) -> None:
        sym = rec.get("symbol")
        if sym is None:
            return
        with self._lock:
            st = self.symbols.get(sym)
            if st is None:
                return
            if st.price == 0.0:
                st.prev_price = rec["price"]
                st.open_price = rec["price"]
            st.price = rec["price"]
            st.volume = rec.get("volume", 0)
            st.bid = rec.get("bid", 0.0)
            st.ask = rec.get("ask", 0.0)
            st.last_ts = rec.get("timestamp", "")
            st.history.append((rec.get("timestamp", ""), rec["price"]))
            st.vol_window.append(rec.get("volume", 0))
            self.tick_count += 1
            self._recent_tick_times.append(time.monotonic())
            self.recent_events.appendleft(
                (rec.get("timestamp", ""), sym, rec["price"])
            )

    def apply_position(self, rec: dict) -> None:
        sym = rec.get("symbol")
        if sym is None:
            return
        with self._lock:
            self.portfolio[sym] = rec

    def apply_signal(self, symbol: str, rec: dict) -> None:
        """Store the latest Flink-computed signal for a symbol."""
        if not symbol:
            return
        with self._lock:
            self.signals[symbol] = rec
            self.signal_count += 1

    # -- readers (called from Streamlit thread) --------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rows = []
            for sym, st in self.symbols.items():
                if st.price == 0.0:
                    continue
                base = st.prev_price or st.price
                change_pct = (st.price - base) / base * 100 if base else 0.0
                avg_vol = (sum(st.vol_window) / len(st.vol_window)) if st.vol_window else 0
                vol_ratio = (st.volume / avg_vol) if avg_vol else 0.0
                sig = self.signals.get(sym)
                rows.append(
                    {
                        "symbol": sym,
                        "price": st.price,
                        "change_pct": change_pct,
                        "volume": st.volume,
                        "vol_ratio": vol_ratio,
                        "bid": st.bid,
                        "ask": st.ask,
                        "last_ts": st.last_ts,
                        # Flink-authoritative fields (None until signals arrive)
                        "flink_change_pct": sig.get("price_change_pct") if sig else None,
                        "flink_vol_ratio": sig.get("volume_ratio") if sig else None,
                        "flink_status": sig.get("status") if sig else None,
                    }
                )
            events = list(self.recent_events)
            portfolio = dict(self.portfolio)
            eps = self._events_per_sec_locked()
            total = self.tick_count
            signals_seen = self.signal_count
        return {
            "rows": rows,
            "events": events,
            "portfolio": portfolio,
            "events_per_sec": eps,
            "tick_count": total,
            "signal_count": signals_seen,
        }

    def history_for(self, symbol: str) -> list[tuple[str, float]]:
        with self._lock:
            st = self.symbols.get(symbol)
            return list(st.history) if st else []

    def _events_per_sec_locked(self) -> float:
        now = time.monotonic()
        window = [t for t in self._recent_tick_times if now - t <= 5.0]
        if len(window) < 2:
            return 0.0
        span = max(window[-1] - window[0], 1e-6)
        return (len(window) - 1) / span


class ConsumerThread(threading.Thread):
    """Background daemon thread that drains Kafka into MarketState."""

    def __init__(self, kafka: KafkaConfig, state: MarketState) -> None:
        super().__init__(daemon=True, name="kafka-consumer")
        self.kafka = kafka
        self.state = state
        self._stop = threading.Event()
        self._sr_deserializers: dict[str, Any] = {}
        self._sr_ctx = None
        self._init_sr()

    def _init_sr(self) -> None:
        """Build Schema Registry deserializers (best-effort; silently falls back
        to plain-JSON decoding if SR isn't configured)."""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.json_schema import JSONDeserializer
            from confluent_kafka.serialization import MessageField, SerializationContext
            from config.settings import SchemaRegistryConfig

            sr = SchemaRegistryConfig.from_env()
            sr.validate()
            client = SchemaRegistryClient(sr.client_conf())
            for topic in (
                self.kafka.topic_market_ticks,
                self.kafka.topic_portfolio_positions,
                self.kafka.topic_market_signals,
            ):
                try:
                    schema_str = client.get_latest_version(f"{topic}-value").schema.schema_str
                    self._sr_deserializers[topic] = JSONDeserializer(
                        schema_str, schema_registry_client=client
                    )
                except Exception:  # noqa: BLE001 -- signals subject may not exist yet
                    pass
            self._sr_ctx = (SerializationContext, MessageField)
        except Exception:  # noqa: BLE001 -- SR optional; fall back to plain JSON
            self._sr_deserializers = {}
            self._sr_ctx = None

    @staticmethod
    def _decode_key_symbol(raw: bytes | None) -> str | None:
        """Decode the market_signals Kafka key (SR-encoded STRING) -> symbol.

        Wire format: [0][4-byte schema id][length-prefixed UTF-8 string].
        Skip the 5-byte SR header + 1 length byte and read the ascii tail.
        """
        if not raw or len(raw) <= 6:
            return None
        try:
            return raw[6:].decode("utf-8", "ignore") or None
        except Exception:  # noqa: BLE001
            return None

    def _decode(self, topic: str, raw: bytes) -> dict | None:
        """Decode a message value: SR-framed (magic byte 0) or plain JSON."""
        if not raw:
            return None
        if raw[0] == 0 and topic in self._sr_deserializers and self._sr_ctx:
            SerializationContext, MessageField = self._sr_ctx
            try:
                return self._sr_deserializers[topic](
                    raw, SerializationContext(topic, MessageField.VALUE)
                )
            except Exception:  # noqa: BLE001
                return None
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return None

    def _make_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": self.kafka.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka.api_key,
            "sasl.password": self.kafka.api_secret,
            # Unique group per session so the dashboard sees ALL partitions live.
            "group.id": f"streamlit-dashboard-{int(time.time() * 1000)}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
        return Consumer(conf)

    def _build_assignment(self, consumer) -> None:
        """Directly assign all partitions with simple, non-blocking offsets.

        We use assign() (not subscribe) so there's no group-rebalance delay.
        Offsets:
          - portfolio_positions : OFFSET_BEGINNING (compacted current state)
          - market_signals      : OFFSET_BEGINNING (upsert; replay quickly)
          - market_ticks        : OFFSET_END (live only)
        """
        from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, TopicPartition

        consumer.poll(0)
        cluster_md = consumer.list_topics(timeout=15)

        offset_for = {
            self.kafka.topic_portfolio_positions: OFFSET_BEGINNING,
            self.kafka.topic_market_signals: OFFSET_BEGINNING,
            self.kafka.topic_market_ticks: OFFSET_END,
        }
        assignment = []
        for topic, off in offset_for.items():
            tmd = cluster_md.topics.get(topic)
            if tmd is None:
                continue
            for p in tmd.partitions.keys():
                assignment.append(TopicPartition(topic, p, off))
        consumer.assign(assignment)

    def run(self) -> None:
        consumer = self._make_consumer()
        self._build_assignment(consumer)
        try:
            # Phase 1: drain the historical backlog (portfolio + signals replay)
            # then mark ready so the UI renders only current values (no flicker).
            idle = 0
            received_any = False
            warmup_deadline = time.monotonic() + 6.0
            while not self._stop.is_set():
                if time.monotonic() > warmup_deadline:
                    break
                msg = consumer.poll(0.2)
                if msg is None or msg.error():
                    if received_any:
                        idle += 1
                        if idle >= 4:  # ~0.8s quiet after data = backlog drained
                            break
                    continue
                idle = 0
                received_any = True
                self._handle(msg)
            self.state.mark_ready()

            # Phase 2: normal live loop
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                self._handle(msg)
        finally:
            consumer.close()

    def _handle(self, msg) -> None:
        topic = msg.topic()
        if topic == self.kafka.topic_market_signals:
            rec = self._decode(topic, msg.value())
            if rec is None:
                return
            symbol = self._decode_key_symbol(msg.key())
            self.state.apply_signal(symbol, rec)
        elif topic == self.kafka.topic_market_ticks:
            rec = self._decode(topic, msg.value())
            if rec is not None:
                self.state.apply_tick(rec)
        elif topic == self.kafka.topic_portfolio_positions:
            rec = self._decode(topic, msg.value())
            if rec is not None:
                self.state.apply_position(rec)

    def stop(self) -> None:
        self._stop.set()


def start_consumer() -> MarketState:
    """Create shared state + start the background consumer thread once."""
    kafka = KafkaConfig.from_env()
    kafka.validate()
    state = MarketState()
    ConsumerThread(kafka, state).start()
    return state
