"""Export the customers who agreed to email, for the mailing provider.

Runs under `art30-emit`, which emits the `processing` facet around it: this job is
the one in ACME's platform whose purpose and legal basis come from the thing that
ran rather than from qedro.yaml.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import duckdb


def main(database: str, out: str) -> None:
    rows = (
        duckdb.connect(database, read_only=True)
        .execute(
            "select c.customer_id, c.email from crm_consent_state s "
            "join crm_customers c using (customer_id) "
            "where s.channel = 'email' and s.granted order by c.customer_id"
        )
        .fetchall()
    )
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "email"])
        writer.writerows(rows)
    print(f"exported {len(rows)} customers who agreed to email")


if __name__ == "__main__":
    main(*sys.argv[1:3])
