"""ACME Finanz GmbH's raw data for one day, the same for the same day every time.

Everything is fictitious and chosen to be seen to be, as in qedro's demo: customers
are Kundin/Kunde 0001 and onwards in Musterstadt, 00000, a postcode Germany never
assigned, with addresses at `.example`, a domain reserved so that no mail to it can
arrive.

The files are cumulative: day N holds every customer and consent event from day 1 to
N, so a later day's dbt run rebuilds the same tables over more rows, the way a daily
pipeline does.
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 11, 2)
CUSTOMERS_AT_START = 40
NEW_CUSTOMERS_PER_DAY = 3
CONSENT_EVENTS_PER_DAY = 8
ORDERS_PER_DAY = 20
CHANNELS = ("email", "phone", "post")


def _customer(number: int, joined: date) -> dict[str, str]:
    title = "Kundin" if number % 2 else "Kunde"
    return {
        "customer_id": f"C{number:04d}",
        "name": f"{title} {number:04d}",
        "email": f"kunde{number:04d}@acme-kunden.example",
        "postcode": "00000",
        "city": "Musterstadt",
        "joined_on": joined.isoformat(),
    }


def rows(day: int) -> dict[str, list[dict[str, str]]]:
    """Every raw row up to and including *day*, seeded by the day, so reproducible."""
    customers = [_customer(n, START) for n in range(1, CUSTOMERS_AT_START + 1)]
    consent: list[dict[str, str]] = []
    orders: list[dict[str, str]] = []
    for d in range(1, day + 1):
        today = START + timedelta(days=d - 1)
        rng = random.Random(f"acme-{d}")
        first = len(customers) + 1
        customers += [_customer(n, today) for n in range(first, first + NEW_CUSTOMERS_PER_DAY)]
        for i in range(CONSENT_EVENTS_PER_DAY):
            consent.append(
                {
                    "event_id": f"E{d:03d}{i:02d}",
                    "customer_id": rng.choice(customers)["customer_id"],
                    "channel": rng.choice(CHANNELS),
                    "granted": rng.choice(("true", "false")),
                    "recorded_on": today.isoformat(),
                }
            )
        for i in range(ORDERS_PER_DAY):
            orders.append(
                {
                    "order_id": f"O{d:03d}{i:03d}",
                    "customer_id": rng.choice(customers)["customer_id"],
                    "amount_eur": f"{rng.randint(500, 50000) / 100:.2f}",
                    "ordered_on": today.isoformat(),
                }
            )
    return {"customers": customers, "consent_events": consent, "orders": orders}


def write(day: int, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, table in rows(day).items():
        with (directory / f"{name}.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(table[0]))
            writer.writeheader()
            writer.writerows(table)
