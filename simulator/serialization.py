"""
Value serialization for the simulator.

Two modes:
  * Schema Registry JSON Schema (default for real runs) -> Flink/Tableflow get
    typed columns and Iceberg gets clean types.
  * Plain JSON bytes (fallback / --no-schema-registry) -> simplest, but Flink
    would need explicit CREATE TABLE with value.format='json'.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from config.settings import PROJECT_ROOT, SchemaRegistryConfig

SCHEMAS_DIR = PROJECT_ROOT / "schemas"

# topic spec key -> schema file. Only topics the SIMULATOR produces are listed.
# `market_signals` is produced by Flink, not this module.
SCHEMA_FILES = {
    "market_ticks": SCHEMAS_DIR / "market_ticks.schema.json",
    "portfolio_positions": SCHEMAS_DIR / "portfolio_positions.schema.json",
}


def _load_schema_str(name: str) -> str:
    return SCHEMA_FILES[name].read_text()


# A serializer takes (payload_dict, topic_name) -> value bytes
Serializer = Callable[[dict, str], bytes]


def plain_json_serializer() -> Serializer:
    def _ser(payload: dict, _topic: str) -> bytes:
        return json.dumps(payload).encode()

    return _ser


def schema_registry_serializers(
    sr_cfg: SchemaRegistryConfig,
) -> dict[str, "object"]:
    """Build per-topic JSONSerializer objects bound to the Schema Registry.

    Returns a dict spec_key -> JSONSerializer. The serializer auto-registers
    the schema under subject "<topic>-value" (TopicNameStrategy) on first use.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.json_schema import JSONSerializer

    sr_cfg.validate()
    client = SchemaRegistryClient(sr_cfg.client_conf())

    serializers: dict[str, object] = {}
    for spec_key in ("market_ticks", "portfolio_positions"):
        schema_str = _load_schema_str(spec_key)
        serializers[spec_key] = JSONSerializer(
            schema_str,
            client,
            conf={"auto.register.schemas": True},
        )
    return serializers
