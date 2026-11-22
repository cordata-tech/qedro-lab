"""Load the provider's price file and the reference data into the warehouse.

Ordinary custom Python, with no lineage in it: the DAG runs it under art30-emit,
which declares what it reads and writes.
"""

from __future__ import annotations

import csv
import os
import sys

import psycopg
import s3fs

sys.path.insert(0, "/lab")
import funds


def main() -> None:
    fs = s3fs.S3FileSystem(endpoint_url=os.environ["AWS_ENDPOINT_URL"])
    with fs.open(funds.PROVIDER_FILE, "r") as f:
        prices = list(csv.DictReader(f))
    with psycopg.connect(funds.DSN) as conn:
        conn.execute("create schema if not exists funds")
        conn.execute("drop table if exists funds.share_classes")
        conn.execute(
            "create table funds.share_classes "
            "(share_class text primary key, fund text, name text, currency text)"
        )
        for row in funds.share_classes():
            conn.execute(
                "insert into funds.share_classes values (%s, %s, %s, %s)",
                (row["share_class"], row["fund"], row["name"], row["currency"]),
            )
        conn.execute("drop table if exists funds.prices")
        conn.execute(
            "create table funds.prices (share_class text, price_date date, nav numeric, "
            "net_assets_eur numeric, costs_eur numeric)"
        )
        for row in prices:
            conn.execute(
                "insert into funds.prices values (%s, %s, %s, %s, %s)",
                (row["share_class"], row["price_date"], row["nav"], row["net_assets_eur"],
                 row["costs_eur"]),
            )  # fmt: skip
    print(f"loaded {len(prices)} prices")


if __name__ == "__main__":
    main()
