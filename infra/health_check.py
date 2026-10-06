"""
Pre-demo health check — run this a few minutes before presenting.

Verifies the whole pipeline is live and consistent so there are no surprises:
  1. Confluent Kafka auth + required topics exist
  2. Simulator is producing market_ticks (offset growing)
  3. Flink job is alive (market_signals offset growing)
  4. Schema Registry reachable + required subjects registered
  5. watsonx.data reachable + all demo queries return data (8 symbols)
  6. Anomaly page currently ALL CLEAR (so the first shock reveal is clean)

Usage:
    python -m infra.health_check
Exit code 0 = all good, 1 = something needs attention.
"""

from __future__ import annotations

import os
import sys
import time

import certifi

os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

from config.settings import KafkaConfig, SchemaRegistryConfig

OK   = "\033[92m✓\033[0m"
BAD  = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"

_failures = 0


def _fail(msg: str) -> None:
    global _failures
    _failures += 1
    print(f"  {BAD} {msg}")


def _ok(msg: str) -> None:
    print(f"  {OK} {msg}")


def _warn(msg: str) -> None:
    print(f"  {WARN} {msg}")


def check_kafka_and_flink() -> None:
    from confluent_kafka import Consumer, TopicPartition

    k = KafkaConfig.from_env()
    conf = {
        "bootstrap.servers": k.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": k.api_key,
        "sasl.password": k.api_secret,
        "group.id": f"health-{time.time()}",
    }
    print("Confluent Kafka + Flink:")
    try:
        c = Consumer(conf)
        md = c.list_topics(timeout=15)
    except Exception as exc:  # noqa: BLE001
        _fail(f"cannot reach Kafka: {exc}")
        return

    for t in (k.topic_market_ticks, k.topic_market_signals, k.topic_portfolio_positions):
        if t in md.topics:
            _ok(f"topic {t} exists")
        else:
            _fail(f"topic {t} MISSING")

    def high(topic: str) -> int:
        tot = 0
        tmd = md.topics.get(topic)
        if not tmd:
            return 0
        for p in tmd.partitions:
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            tot += hi
        return tot

    # Simulator producing?
    a = high(k.topic_market_ticks)
    time.sleep(4)
    b = high(k.topic_market_ticks)
    if b > a:
        _ok(f"simulator producing market_ticks (+{b - a} in 4s)")
    else:
        _fail("market_ticks NOT growing — is the simulator running?")

    # Flink alive?
    a = high(k.topic_market_signals)
    time.sleep(5)
    b = high(k.topic_market_signals)
    if b > a:
        _ok(f"Flink job alive (market_signals +{b - a})")
    else:
        _fail(
            "market_signals NOT growing — Flink INSERT job may be STOPPED. "
            "Re-run flink/01_market_signals.sql INSERT."
        )
    c.close()


def check_schema_registry() -> None:
    print("Schema Registry:")
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr = SchemaRegistryConfig.from_env()
        sr.validate()
        client = SchemaRegistryClient(sr.client_conf())
        subs = client.get_subjects()
        need = {"market_ticks-value", "market_signals-value"}
        if need.issubset(set(subs)):
            _ok(f"schemas registered ({len(subs)} subjects)")
        else:
            _warn(f"expected subjects missing: {need - set(subs)}")
    except Exception as exc:  # noqa: BLE001
        _fail(f"Schema Registry error: {exc}")


def check_watsonx() -> None:
    print("watsonx.data (Presto):")
    try:
        from dashboard.watsonx_client import (
            ANOMALY_SIGMA_THRESHOLD,
            OVEREXPOSURE_THRESHOLD,
            list_symbols,
            q_current_vs_historical,
            q_portfolio_rebalance,
            run_query,
        )
    except Exception as exc:  # noqa: BLE001
        _fail(f"cannot import client: {exc}")
        return

    try:
        syms = run_query(list_symbols())
        n = len(syms)
        (_ok if n == 8 else _warn)(f"symbols table: {n} rows (expect 8)")

        a = run_query(q_current_vs_historical())
        anoms = int((a["anomaly_sigma"] >= ANOMALY_SIGMA_THRESHOLD).sum())
        if len(a) == 8:
            _ok(f"anomaly query OK ({len(a)} rows)")
        else:
            _warn(
                f"anomaly query returned {len(a)} rows (expect 8 — "
                "Tableflow may still be committing new prices)"
            )
        if anoms == 0:
            _ok("currently ALL CLEAR (clean first-shock reveal)")
        else:
            top = a.iloc[0]
            _warn(
                f"{anoms} anomaly(ies) already showing (e.g. {top['symbol']} "
                f"{top['anomaly_sigma']:.1f}σ) — a prior shock is active; "
                "wait for it to recover or proceed."
            )

        r = run_query(q_portfolio_rebalance())
        over = r[r["weight_pct"] > OVEREXPOSURE_THRESHOLD * 100]["symbol"].tolist()
        if over:
            _ok(f"rebalance query OK; overexposed: {over}")
        else:
            _warn("rebalance query OK but no overexposed holding (expected NVDA ~30%)")

    except Exception as exc:  # noqa: BLE001
        _fail(f"watsonx.data query failed: {exc}")


def main() -> None:
    print("\n=== Demo pre-flight health check ===\n")
    check_kafka_and_flink()
    check_schema_registry()
    check_watsonx()
    print()
    if _failures:
        print(f"{BAD} {_failures} issue(s) need attention before the demo.\n")
        sys.exit(1)
    print(f"{OK} All systems go. You're clear to present.\n")


if __name__ == "__main__":
    main()
