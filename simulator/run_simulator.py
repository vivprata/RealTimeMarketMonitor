"""
Phase 2 — Market simulator runner.

Streams simulated market ticks to Confluent Cloud Kafka. Wraps the pure
PriceEngine (engine.py) with a confluent_kafka Producer.

Key behaviors:
  * --warmup N     Back-fill N minutes of history at burst speed BEFORE going
                   live, so Flink's rolling window is warm.
  * --scenario     normal | rally | selloff (starting scenario)
  * --shock-after  Automatically trigger the shock after N seconds of
                   normal trading (great for a hands-free rehearsal).
  * SIGUSR1        Send `kill -USR1 <pid>` to trigger the shock live
                   (this is what the Streamlit "INJECT MARKET SHOCK" button calls).
  * --dry-run      Print JSON to stdout instead of producing to Kafka
                   (validate the whole thing with no cloud creds).
  * --seed-portfolio  Produce portfolio_positions once at startup.

Examples:
    # Validate with no cloud creds:
    python -m simulator.run_simulator --dry-run --duration 3 --shock-after 1

    # Real run against Confluent Cloud:
    python -m simulator.run_simulator --warmup 10 --seed-portfolio
    # ...then trigger the shock in another terminal:
    kill -USR1 $(pgrep -f run_simulator)
"""

from __future__ import annotations

import argparse
import json
import random as _random
import signal
import sys
import time
from datetime import datetime, timezone

from config.settings import KafkaConfig, PORTFOLIO_ID, SYMBOLS, SimConfig
from simulator.engine import PriceEngine, Scenario


class _NullProducer:
    """Stand-in producer for --dry-run: prints instead of sending."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.count = 0

    def produce(self, topic, key=None, value=None, **_):
        self.count += 1
        if self.verbose:
            print(f"[{topic}] {value.decode() if isinstance(value, bytes) else value}")

    def poll(self, _timeout=0):
        return 0

    def flush(self, _timeout=None):
        return 0


def _make_producer(dry_run: bool, kafka: KafkaConfig, quiet: bool):
    if dry_run:
        return _NullProducer(verbose=not quiet)
    from confluent_kafka import Producer  # lazy: dry-run needs no creds

    kafka.validate()
    return Producer(kafka.producer_conf())


def _delivery_report(err, msg):
    if err is not None:
        print(f"  DELIVERY FAILED: {err}", file=sys.stderr)


def _build_value_encoder(dry_run: bool, use_sr: bool, kafka: KafkaConfig):
    """Return encode(payload, topic, spec_key) -> value bytes/str."""
    from simulator.serialization import plain_json_serializer

    if dry_run or not use_sr:
        plain = plain_json_serializer()
        return lambda payload, topic, spec_key: plain(payload, topic)

    from confluent_kafka.serialization import MessageField, SerializationContext
    from simulator.serialization import schema_registry_serializers
    from config.settings import SchemaRegistryConfig

    serializers = schema_registry_serializers(SchemaRegistryConfig.from_env())

    def _encode(payload: dict, topic: str, spec_key: str) -> bytes:
        ser = serializers[spec_key]
        return ser(payload, SerializationContext(topic, MessageField.VALUE))

    return _encode


def _produce(producer, topic: str, key: str, value, dry_run: bool):
    if dry_run:
        producer.produce(topic, key=key, value=value)
    else:
        producer.produce(
            topic, key=key.encode(), value=value, callback=_delivery_report
        )


def seed_portfolio(producer, kafka: KafkaConfig, encode, dry_run: bool) -> None:
    """Produce one record per holding to portfolio_positions (keyed by symbol)."""
    print(f"Seeding portfolio_positions ({len(SYMBOLS)} holdings)...")
    for s in SYMBOLS:
        payload = {
            "portfolio_id": PORTFOLIO_ID,
            "symbol": s.symbol,
            "shares": s.shares,
            "purchase_price": s.purchase_price,
        }
        value = encode(payload, kafka.topic_portfolio_positions, "portfolio_positions")
        _produce(producer, kafka.topic_portfolio_positions, s.symbol, value, dry_run)
    producer.flush(10)


def main() -> None:
    parser = argparse.ArgumentParser(description="Market simulator -> Confluent Cloud.")
    parser.add_argument("--scenario", choices=[s.value for s in Scenario],
                        default="normal", help="Starting scenario")
    parser.add_argument("--duration", type=float, default=0,
                        help="Run for N seconds then stop (0 = run forever)")
    parser.add_argument("--tps", type=int, default=None,
                        help="Ticks/sec across all symbols (default: SIM_TICKS_PER_SECOND)")
    parser.add_argument("--warmup", type=int, default=None,
                        help="Backfill N minutes of history before going live "
                             "(default: SIM_WARMUP_MINUTES)")
    parser.add_argument("--shock-after", type=float, default=0,
                        help="Auto-trigger a shock after N seconds (0 = never)")
    parser.add_argument("--shock-symbol", default="IBM",
                        help="Symbol the MANUAL trigger (SIGUSR1 / dashboard button) "
                             "shocks. Default IBM — a low-vol name that reliably "
                             "crosses the anomaly threshold.")
    parser.add_argument("--random-shocks", action="store_true",
                        help="Auto-fire shocks on RANDOM symbols at random intervals "
                             "for a live/unpredictable market feel.")
    parser.add_argument("--random-shock-min", type=float, default=45,
                        help="Min seconds between random background shocks (default 45)")
    parser.add_argument("--random-shock-max", type=float, default=120,
                        help="Max seconds between random background shocks (default 120)")
    parser.add_argument("--shock-duration", type=int, default=None,
                        help="Ticks over which the shock DROP unfolds (hold+recover added)")
    parser.add_argument("--seed-portfolio", action="store_true",
                        help="Produce portfolio_positions once at startup")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print JSON instead of producing to Kafka (no creds needed)")
    parser.add_argument("--no-schema-registry", action="store_true",
                        help="Produce plain JSON bytes instead of Schema Registry JSON Schema")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-tick output")
    args = parser.parse_args()

    sim = SimConfig()
    tps = args.tps or sim.ticks_per_second
    warmup = sim.warmup_minutes if args.warmup is None else args.warmup
    n_symbols = len(SYMBOLS)

    kafka = KafkaConfig.from_env()
    producer = _make_producer(args.dry_run, kafka, args.quiet)
    engine = PriceEngine(seed=sim.seed)

    use_sr = not args.no_schema_registry
    encode = _build_value_encoder(args.dry_run, use_sr, kafka)

    # SIGUSR1 -> trigger shock live (used by the Streamlit button).
    shock_requested: dict[str, bool] = {"flag": False}

    def _on_sigusr1(_sig, _frame):
        shock_requested["flag"] = True

    signal.signal(signal.SIGUSR1, _on_sigusr1)

    print(
        f"Market simulator starting "
        f"(dry_run={args.dry_run}, tps={tps}, symbols={n_symbols}, "
        f"scenario={args.scenario}, schema_registry={use_sr and not args.dry_run})"
    )

    if args.seed_portfolio:
        seed_portfolio(producer, kafka, encode, args.dry_run)

    # --- Warmup: backfill history so Flink's window is primed ---
    if warmup > 0:
        print(f"Backfilling {warmup} min of warmup history (burst)...")
        warm = engine.warmup_ticks(minutes=warmup)
        for t in warm:
            value = encode(t.to_dict(), kafka.topic_market_ticks, "market_ticks")
            _produce(producer, kafka.topic_market_ticks, t.symbol, value, args.dry_run)
            producer.poll(0)
        producer.flush(30)
        print(f"  warmup complete: {len(warm)} ticks back-dated before now")

    # --- Live streaming loop ---
    # Set scenario first, THEN arm MSFT anomaly so set_scenario doesn't clear it.
    engine.set_scenario(Scenario(args.scenario))
    engine.arm_msft_anomaly()
    print(f"[demo] MSFT permanent anomaly armed at "
          f"${engine._shocks['MSFT']['anchor'] * (1 - 0.063):.2f} "
          f"(-6.3% = ~5.0σ). Flink will detect HIGH_VOLUME_DROP.")
    sleep_between_batches = max(0.0, (1.0 / max(tps / n_symbols, 0.001)))
    start = time.monotonic()
    produced = 0
    auto_shock_done = False

    _rng = _random.Random()
    next_random_shock = (
        start + _rng.uniform(args.random_shock_min, args.random_shock_max)
        if args.random_shocks else None
    )

    print(
        "Streaming live ticks. Ctrl-C to stop."
        + (f" Auto-shock at +{args.shock_after}s." if args.shock_after
           else " Trigger shock with: kill -USR1 <pid> (repeatable).")
        + (" Random background shocks ON." if args.random_shocks else "")
    )

    try:
        while True:
            now_mono = time.monotonic()
            elapsed = now_mono - start

            if args.duration and elapsed >= args.duration:
                break

            # 1) Manual trigger (SIGUSR1 / dashboard button) -> configured symbol
            if shock_requested["flag"]:
                shock_requested["flag"] = False  # re-arm for next time
                target = (None if (args.shock_symbol or "").lower() == "random"
                          else args.shock_symbol)
                sym = engine.trigger_shock(symbol=target, drop_ticks=args.shock_duration)
                print(f">>> MANUAL SHOCK on {sym} <<<")
            elif args.shock_after and not auto_shock_done and elapsed >= args.shock_after:
                auto_shock_done = True
                target = (None if (args.shock_symbol or "").lower() == "random"
                          else args.shock_symbol)
                sym = engine.trigger_shock(symbol=target, drop_ticks=args.shock_duration)
                print(f">>> TIMED SHOCK on {sym} <<<")

            # 2) Random background shock -> random symbol, random cadence
            if next_random_shock is not None and now_mono >= next_random_shock:
                sym = engine.trigger_shock(symbol=None)
                print(f">>> RANDOM BACKGROUND SHOCK on {sym} <<<")
                next_random_shock = now_mono + _rng.uniform(
                    args.random_shock_min, args.random_shock_max)

            ts = datetime.now(timezone.utc)
            for t in engine.step_all(ts):
                value = encode(t.to_dict(), kafka.topic_market_ticks, "market_ticks")
                _produce(producer, kafka.topic_market_ticks, t.symbol, value, args.dry_run)
                produced += 1
            producer.poll(0)
            time.sleep(sleep_between_batches)

    except KeyboardInterrupt:
        print("\nStopping (Ctrl-C).")
    finally:
        producer.flush(30)
        print(
            f"Done. Produced {produced} live ticks"
            + (f" (+{producer.count} dry-run msgs)" if args.dry_run else "")
            + "."
        )


if __name__ == "__main__":
    main()
