"""Compute each share class's ongoing charges figure from the day's prices.

The figure a fund document publishes: the day's costs, annualised, as a share of net
assets. Ordinary custom Python, run under art30-emit by the DAG.
"""

from __future__ import annotations

import sys

import psycopg

sys.path.insert(0, "/lab")
import funds


def main() -> None:
    with psycopg.connect(funds.DSN) as conn:
        conn.execute("drop table if exists funds.ongoing_charges")
        conn.execute(
            "create table funds.ongoing_charges as "
            "select s.share_class, s.fund, s.name, p.price_date, p.nav, "
            "round(p.costs_eur * 365 / p.net_assets_eur * 100, 2) as ongoing_charges_pct "
            "from funds.prices p join funds.share_classes s using (share_class)"
        )
        n = conn.execute("select count(*) from funds.ongoing_charges").fetchone()[0]
    print(f"computed ongoing charges for {n} share classes")


if __name__ == "__main__":
    main()
