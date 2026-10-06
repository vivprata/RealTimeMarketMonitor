"""
Shared configuration for the Confluent + watsonx.data stock market demo.

Single source of truth for:
  - the symbol universe (loaded from config/symbols.json)
  - environment-driven settings (Confluent Cloud, S3, simulator behavior)

Both the data-generation scripts and the market simulator import from here so
prices, volumes and the portfolio stay consistent across historical data and the
live stream.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Project layout ----------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
SYMBOLS_FILE = CONFIG_DIR / "symbols.json"

# Load .env once on import (real .env overrides shell env only if not set).
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Symbol:
    symbol: str
    company_name: str
    sector: str
    industry: str
    base_price: float
    annual_volatility: float
    beta: float
    avg_daily_volume: int
    shares: int
    purchase_price: float


def load_symbols() -> list[Symbol]:
    """Load the symbol universe from config/symbols.json."""
    with SYMBOLS_FILE.open() as fh:
        raw = json.load(fh)
    return [Symbol(**item) for item in raw["symbols"]]


def load_portfolio_id() -> str:
    with SYMBOLS_FILE.open() as fh:
        return json.load(fh)["portfolio_id"]


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    return int(val) if val not in (None, "") else default


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    api_key: str
    api_secret: str
    topic_market_ticks: str
    topic_market_events: str
    topic_market_signals: str
    topic_portfolio_positions: str

    def producer_conf(self) -> dict[str, Any]:
        """confluent_kafka producer config for Confluent Cloud (SASL_SSL)."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.api_key,
            "sasl.password": self.api_secret,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
            "client.id": "market-simulator",
        }

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        return cls(
            bootstrap_servers=_env("CONFLUENT_BOOTSTRAP_SERVERS", ""),
            api_key=_env("CONFLUENT_API_KEY", ""),
            api_secret=_env("CONFLUENT_API_SECRET", ""),
            topic_market_ticks=_env("TOPIC_MARKET_TICKS", "market_ticks"),
            topic_market_events=_env("TOPIC_MARKET_EVENTS", "market_events"),
            topic_market_signals=_env("TOPIC_MARKET_SIGNALS", "market_signals"),
            topic_portfolio_positions=_env(
                "TOPIC_PORTFOLIO_POSITIONS", "portfolio_positions"
            ),
        )

    def validate(self) -> None:
        missing = [
            name
            for name, val in {
                "CONFLUENT_BOOTSTRAP_SERVERS": self.bootstrap_servers,
                "CONFLUENT_API_KEY": self.api_key,
                "CONFLUENT_API_SECRET": self.api_secret,
            }.items()
            if not val
        ]
        if missing:
            raise RuntimeError(
                "Missing Confluent Cloud settings: "
                + ", ".join(missing)
                + ". Copy .env.example to .env and fill in your cluster keys."
            )


@dataclass
class SchemaRegistryConfig:
    url: str
    api_key: str
    api_secret: str

    def client_conf(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "basic.auth.user.info": f"{self.api_key}:{self.api_secret}",
        }

    @classmethod
    def from_env(cls) -> "SchemaRegistryConfig":
        return cls(
            url=_env("SCHEMA_REGISTRY_URL", ""),
            api_key=_env("SCHEMA_REGISTRY_API_KEY", ""),
            api_secret=_env("SCHEMA_REGISTRY_API_SECRET", ""),
        )

    def validate(self) -> None:
        missing = [
            name
            for name, val in {
                "SCHEMA_REGISTRY_URL": self.url,
                "SCHEMA_REGISTRY_API_KEY": self.api_key,
                "SCHEMA_REGISTRY_API_SECRET": self.api_secret,
            }.items()
            if not val
        ]
        if missing:
            raise RuntimeError(
                "Missing Schema Registry settings: "
                + ", ".join(missing)
                + ". Create a Schema Registry API key (separate from the cluster key)."
            )


@dataclass
class S3Config:
    region: str
    bucket: str
    prefix: str
    access_key_id: str
    secret_access_key: str

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(
            region=_env("AWS_REGION", "us-east-1"),
            bucket=_env("S3_BUCKET", ""),
            prefix=_env("S3_PREFIX", "stock-demo").strip("/"),
            access_key_id=_env("AWS_ACCESS_KEY_ID", ""),
            secret_access_key=_env("AWS_SECRET_ACCESS_KEY", ""),
        )

    def validate(self) -> None:
        if not self.bucket:
            raise RuntimeError("S3_BUCKET is not set in .env")


@dataclass
class WatsonxConfig:
    host: str
    port: int
    user: str
    password: str
    catalog_streaming: str
    schema_streaming: str
    catalog_historical: str
    schema_historical: str

    @classmethod
    def from_env(cls) -> "WatsonxConfig":
        return cls(
            host=_env("WXD_HOST", ""),
            port=_env_int("WXD_PORT", 443),
            user=_env("WXD_USER", ""),
            password=_env("WXD_PASSWORD", ""),
            catalog_streaming=_env("WXD_CATALOG_STREAMING", "confluent"),
            schema_streaming=_env("WXD_SCHEMA_STREAMING", ""),
            catalog_historical=_env("WXD_CATALOG_HISTORICAL", "hive_catalog"),
            schema_historical=_env("WXD_SCHEMA_HISTORICAL", "stock_demo"),
        )

    def validate(self) -> None:
        missing = [
            name
            for name, val in {
                "WXD_HOST": self.host,
                "WXD_USER": self.user,
                "WXD_PASSWORD": self.password,
                "WXD_SCHEMA_STREAMING": self.schema_streaming,
            }.items()
            if not val
        ]
        if missing:
            raise RuntimeError(
                "Missing watsonx.data settings: " + ", ".join(missing)
                + ". Fill in the WXD_* values in .env — WXD_SCHEMA_STREAMING "
                "must be your new Confluent cluster ID (lkc-xxxxxx)."
            )


@dataclass
class SimConfig:
    ticks_per_second: int = field(default_factory=lambda: _env_int("SIM_TICKS_PER_SECOND", 18))
    warmup_minutes: int = field(default_factory=lambda: _env_int("SIM_WARMUP_MINUTES", 10))
    seed: int = field(default_factory=lambda: _env_int("SIM_SEED", 42))


# Convenience singletons for scripts that just want defaults.
SYMBOLS: list[Symbol] = load_symbols()
PORTFOLIO_ID: str = load_portfolio_id()
