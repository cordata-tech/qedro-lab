"""ACME's fund-data service: three made-up funds, two share classes each, and the
price file a data provider delivers every day, generated the way generate.py makes
ACME's customer data, so day N is always the same file.

It stands for a regulatory-reporting product for funds: a provider's prices come in,
custom Python loads and checks them, computes a figure per share class, and publishes
one document per fund. The jobs are in jobs/fund_*.py and the DAG that runs them is
airflow/dags/fund_feed.py. Every name and number here is fictitious; the identifiers
are not ISINs.
"""

from __future__ import annotations

import csv
import io
from datetime import date, timedelta

import yaml

START = date(2026, 11, 2)

#: Fund id to name. `MF` for Musterfonds, as ACME's customers are Kundin 0001 onwards.
FUNDS = {
    "MF0001": "ACME Musterfonds Global",
    "MF0002": "ACME Musterfonds Renten",
    "MF0003": "ACME Musterfonds Nachhaltig",
}

#: Share class suffix to currency: A for retail, I for institutional.
CLASSES = {"A": "EUR", "I": "EUR"}

PROVIDER_FILE = "s3://funds/provider/prices.csv"
WAREHOUSE = "postgres://warehouse:5432/acme.funds"
DSN = "postgresql://acme:acme@warehouse:5432/acme"


def share_classes() -> list[dict[str, str]]:
    """The reference data: every share class the service reports on."""
    return [
        {"share_class": f"{fund}-{suffix}", "fund": fund, "name": name, "currency": currency}
        for fund, name in FUNDS.items()
        for suffix, currency in CLASSES.items()
    ]


def missing_on(day: int, scenario: str = "/lab/scenario.yaml") -> set[str]:
    """Share classes the provider leaves out of day *day*'s file, from the scenario's
    `funds` lines: the exception a check has to catch."""
    with open(scenario, encoding="utf-8") as f:
        steps = yaml.safe_load(f) or []
    return {
        share_class
        for step in steps
        if step.get("day") == day and "funds" in step
        for share_class in step["funds"].get("missing", [])
    }


def prices(day: int, missing: frozenset[str] | set[str] = frozenset()) -> str:
    """Day *day*'s provider file as CSV: a NAV, net assets and the day's costs per
    share class, moving a little each day so a figure changes from one to the next."""
    on = (START + timedelta(days=day - 1)).isoformat()
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["share_class", "price_date", "nav", "net_assets_eur", "costs_eur"])
    for i, row in enumerate(share_classes()):
        if row["share_class"] in missing:
            continue
        nav = 100 + 2 * i + 0.15 * day * (1 + i % 3)
        assets = 50_000_000 * (1 + i) + 250_000 * day
        # Institutional classes are cheaper, as they are in life.
        rate = 0.0012 if row["share_class"].endswith("-I") else 0.0018
        costs = assets * rate / 365
        writer.writerow([row["share_class"], on, f"{nav:.4f}", f"{assets:.2f}", f"{costs:.2f}"])
    return out.getvalue()
