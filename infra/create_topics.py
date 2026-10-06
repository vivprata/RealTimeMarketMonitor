"""
Phase 3 — Create Kafka topics on Confluent Cloud.

Creates the topics the demo needs, with sensible partition/retention settings:

  market_ticks          high-volume live feed
  portfolio_positions   compacted (latest position per symbol)
  market_events         low-volume narrative events

NOTE: `market_signals` is intentionally NOT created here. It is owned/created
by Flink (CREATE TABLE in flink/01_market_signals.sql) so it gets the correct
typed json-registry schema. Pre-creating it as a plain topic causes Flink to
see [key: BYTES, val: BYTES] and the INSERT fails with a schema mismatch.

Usage:
    python -m infra.create_topics                 # create (idempotent)
    python -m infra.create_topics --dry-run       # show plan only
    python -m infra.create_topics --list          # list existing topics
    python -m infra.create_topics --recreate      # purge and recreate all
    python -m infra.create_topics --recreate market_ticks  # purge one topic
"""

from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import KafkaConfig

# topic spec -> (partitions, config overrides)
TOPIC_SPECS: dict[str, dict] = {
    "market_ticks": {
        "partitions": 6,
        "config": {
            "cleanup.policy": "delete",
            "retention.ms": str(24 * 60 * 60 * 1000),  # 1 day
        },
    },
    "portfolio_positions": {
        "partitions": 3,
        "config": {
            "cleanup.policy": "compact",
            "min.cleanable.dirty.ratio": "0.1",
        },
    },
    "market_events": {
        "partitions": 1,
        "config": {
            "cleanup.policy": "delete",
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
        },
    },
}


def _admin(kafka: KafkaConfig) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": kafka.api_key,
            "sasl.password": kafka.api_secret,
        }
    )


def _topic_names(kafka: KafkaConfig) -> dict[str, str]:
    """Map spec keys to the actual configured topic names from env."""
    return {
        "market_ticks": kafka.topic_market_ticks,
        "portfolio_positions": kafka.topic_portfolio_positions,
        "market_events": kafka.topic_market_events,
    }


def list_topics(kafka: KafkaConfig) -> None:
    md = _admin(kafka).list_topics(timeout=15)
    print(f"Cluster: {md.cluster_id}  brokers: {len(md.brokers)}")
    if not md.topics:
        print("Topics: (none)")
        return
    print("Topics:")
    for name in sorted(md.topics):
        t = md.topics[name]
        print(f"  {name}  (partitions={len(t.partitions)})")


def _create_specs(
    admin: AdminClient, names: dict[str, str], spec_keys: list[str]
) -> bool:
    """Create the given spec topics. Returns True on success."""
    to_create = [
        NewTopic(
            names[sk],
            num_partitions=TOPIC_SPECS[sk]["partitions"],
            replication_factor=3,
            config=TOPIC_SPECS[sk]["config"],
        )
        for sk in spec_keys
    ]
    futures = admin.create_topics(to_create)
    ok = True
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  CREATED  {name}")
        except Exception as exc:  # noqa: BLE001
            ok = False
            print(f"  FAILED   {name}: {exc}", file=sys.stderr)
    return ok


def _recreate(
    admin: AdminClient, names: dict[str, str], requested: list[str]
) -> None:
    """Delete then recreate topics (purges stale records)."""
    import time

    if requested:
        spec_keys = [
            sk for sk in TOPIC_SPECS if names[sk] in requested or sk in requested
        ]
        if not spec_keys:
            sys.exit(f"None of {requested} match known topics {list(names.values())}")
    else:
        spec_keys = list(TOPIC_SPECS)

    target_names = [names[sk] for sk in spec_keys]
    print(f"Recreating topics: {target_names}")
    print("Deleting...")
    del_futures = admin.delete_topics(target_names, operation_timeout=30)
    for name, fut in del_futures.items():
        try:
            fut.result()
            print(f"  DELETED  {name}")
        except Exception as exc:  # noqa: BLE001
            print(f"  (delete)  {name}: {exc}")

    print("Waiting for deletion to settle...")
    for _ in range(30):
        existing = set(admin.list_topics(timeout=15).topics.keys())
        if not any(n in existing for n in target_names):
            break
        time.sleep(2)

    print("Recreating...")
    if _create_specs(admin, names, spec_keys):
        print("Done. Topics recreated empty.")
    else:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Confluent Cloud topics.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show plan, create nothing")
    parser.add_argument("--list", action="store_true",
                        help="List existing topics and exit")
    parser.add_argument("--recreate", nargs="*", metavar="TOPIC",
                        help="Delete then recreate topics (all if none given). "
                             "Use to purge stale/plain-JSON records before Flink.")
    args = parser.parse_args()

    kafka = KafkaConfig.from_env()
    try:
        kafka.validate()
    except RuntimeError as exc:
        sys.exit(f"Config error: {exc}")

    admin = _admin(kafka)
    names = _topic_names(kafka)

    if args.recreate is not None:
        _recreate(admin, names, args.recreate)
        return

    if args.list:
        list_topics(kafka)
        return

    existing = set(admin.list_topics(timeout=15).topics.keys())

    to_create: list[NewTopic] = []
    print("Plan:")
    for spec_key, spec in TOPIC_SPECS.items():
        name = names[spec_key]
        if name in existing:
            print(f"  EXISTS  {name}")
            continue
        print(f"  CREATE  {name}  partitions={spec['partitions']}  {spec['config']}")
        to_create.append(
            NewTopic(
                name,
                num_partitions=spec["partitions"],
                replication_factor=3,
                config=spec["config"],
            )
        )

    if args.dry_run:
        print("\nDry run: no topics created.")
        return
    if not to_create:
        print("\nAll topics already exist. Nothing to do.")
        return

    print(f"\nCreating {len(to_create)} topic(s)...")
    futures = admin.create_topics(to_create)
    failed = False
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  OK      {name}")
        except Exception as exc:  # noqa: BLE001
            failed = True
            print(f"  FAILED  {name}: {exc}", file=sys.stderr)

    if failed:
        sys.exit(1)
    print("\nDone.")


if __name__ == "__main__":
    main()
