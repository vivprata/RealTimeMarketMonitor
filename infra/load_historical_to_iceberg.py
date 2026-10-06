"""
Load historical Parquet data from S3 into watsonx.data Iceberg tables via SQL INSERT.

Reads each Parquet file from S3, then inserts all rows in a single INSERT VALUES
statement per table. Uses retry logic for resilience against transient failures.

Usage:
    python -m infra.load_historical_to_iceberg
    python -m infra.load_historical_to_iceberg --dry-run
    python -m infra.load_historical_to_iceberg --table historical_prices
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time

import boto3
import certifi
import pyarrow.parquet as pq
import trino

os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())

from config.settings import S3Config, WatsonxConfig

BUCKET   = "confluent-bucket-us-east"
CATALOG  = "confluent"
SCHEMA   = "stock_demo"
BATCH    = 100   # small batches — more reliable than large ones over HTTPS
RETRIES  = 3     # retry each batch on failure

TABLES = {
    "historical_prices":       "stock-demo/historical_prices/historical_prices.parquet",
    "historical_risk_metrics": "stock-demo/historical_risk_metrics/historical_risk_metrics.parquet",
    "company_reference":       "stock-demo/company_reference/company_reference.parquet",
    "portfolio_positions":     "stock-demo/portfolio_positions/portfolio_positions.parquet",
}


def _s3_client(cfg: S3Config):
    return boto3.client(
        "s3",
        region_name=cfg.region,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
    )


def _presto_conn(cfg: WatsonxConfig):
    return trino.dbapi.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(cfg.user, cfg.password),
        http_headers={"X-Presto-User": cfg.user},
        request_timeout=300,  # 5 min timeout per query
    )


def _read_parquet(s3, key: str):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    return pq.read_table(buf).to_pandas()


def _escape(val) -> str:
    if val is None or (hasattr(val, '__class__') and val.__class__.__name__ == 'float' and val != val):
        return "NULL"
    import math
    if isinstance(val, float) and math.isnan(val):
        return "NULL"
    if isinstance(val, str):
        return "'" + val.replace("'", "''") + "'"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    return str(val)


def _insert_with_retry(cur, table: str, cols: str, rows_sql: str, start: int, end: int) -> None:
    sql = f"INSERT INTO {CATALOG}.{SCHEMA}.{table} ({cols}) VALUES {rows_sql}"
    for attempt in range(1, RETRIES + 1):
        try:
            cur.execute(sql)
            cur.fetchall()  # consume result to confirm commit
            print(f"    ✓ rows {start}–{end}")
            return
        except Exception as e:
            if attempt < RETRIES:
                print(f"    ✗ rows {start}–{end} attempt {attempt} failed: {e} — retrying in 5s...")
                time.sleep(5)
            else:
                raise RuntimeError(f"Failed after {RETRIES} attempts: {e}") from e


def load_table(cur, name: str, df, dry_run: bool) -> None:
    cols = ", ".join(df.columns)
    total = len(df)
    print(f"  Truncating {CATALOG}.{SCHEMA}.{name} ...")
    if not dry_run:
        cur.execute(f"DELETE FROM {CATALOG}.{SCHEMA}.{name}")
        cur.fetchall()

    print(f"  Inserting {total} rows in batches of {BATCH} ...")
    for start in range(0, total, BATCH):
        chunk = df.iloc[start: start + BATCH]
        rows_sql = ", ".join(
            "(" + ", ".join(_escape(v) for v in row) + ")"
            for row in chunk.itertuples(index=False, name=None)
        )
        if dry_run:
            print(f"    (dry-run) rows {start}–{start + len(chunk) - 1}")
        else:
            _insert_with_retry(cur, name, cols, rows_sql, start, start + len(chunk) - 1)

    print(f"  Done — {total} rows loaded into {CATALOG}.{SCHEMA}.{name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", help="Load only this table (default: all)")
    args = parser.parse_args()

    s3_cfg  = S3Config.from_env()
    wxd_cfg = WatsonxConfig.from_env()
    s3      = _s3_client(s3_cfg)
    conn    = _presto_conn(wxd_cfg)
    cur     = conn.cursor()

    tables = {args.table: TABLES[args.table]} if args.table else TABLES

    for name, key in tables.items():
        print(f"\n{'DRY-RUN: ' if args.dry_run else ''}Loading {name}")
        df = _read_parquet(s3, key)
        print(f"  {len(df)} rows, columns: {list(df.columns)}")
        load_table(cur, name, df, args.dry_run)

    print("\nAll tables loaded successfully." if not args.dry_run else "\nDry-run complete.")


if __name__ == "__main__":
    main()
