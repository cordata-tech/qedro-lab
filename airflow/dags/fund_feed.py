"""ACME's fund-data service: a provider's price file to one document per fund.

    deliver_prices ──> ingest_prices ──> check_prices ──> ongoing_charges ──> kid_reports

The shape of a regulatory-reporting product for funds, rehearsed for a pilot with a
team that runs Airflow, custom Python in containers, S3 and Postgres, and emits no
lineage today. What such a team changes to be read by qedro is all in this file:

- each job's command is prefixed with `art30-emit --job … --reads … --writes … --`,
  which emits what it read and wrote around the unchanged job (no purpose or legal
  basis: the data is not personal);
- the check reports its results with the OpenLineage client (jobs/fund_check.py);
- `OPENLINEAGE_CONFIG` points the jobs at a file transport (openlineage-funds.yml),
  and the parent macros name the Airflow task each job ran under.

`deliver_prices` stands for the provider's delivery and emits nothing.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

TOOLS = "/opt/tools/bin"
PRICES = "s3://funds/provider/prices.csv"
DB = "postgres://warehouse:5432/acme.funds"
FUNDS = ("MF0001", "MF0002", "MF0003")

# What every job's command starts with: the parent run, and the feed's own transport.
LINEAGE = (
    "OPENLINEAGE_CONFIG=/lab/openlineage-funds.yml "
    "OPENLINEAGE_PARENT_ID={{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }} "
    "OPENLINEAGE_ROOT_PARENT_ID="
    "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }} "
)


def wrapped(job: str, reads: list[str], writes: list[str], command: str) -> str:
    """The job's command as it ran before, with art30-emit in front of it."""
    declared = " ".join([*(f"--reads {r}" for r in reads), *(f"--writes {w}" for w in writes)])
    return f"{LINEAGE}{TOOLS}/art30-emit --job acme.funds/{job} {declared} -- {command}"


with DAG(
    dag_id="fund_feed",
    start_date=datetime(2026, 11, 2),
    schedule=None,
    catchup=False,
    tags=["acme", "funds"],
):
    deliver = BashOperator(
        task_id="deliver_prices",
        bash_command=f"{TOOLS}/python /lab/jobs/fund_provider.py {{{{ ds }}}}",
        append_env=True,
    )
    ingest = BashOperator(
        task_id="ingest_prices",
        bash_command=wrapped(
            "prices-ingest",
            [PRICES],
            [f"{DB}.prices", f"{DB}.share_classes"],
            f"{TOOLS}/python /lab/jobs/fund_ingest.py",
        ),
        append_env=True,
    )
    check = BashOperator(
        task_id="check_prices",
        bash_command=f"{LINEAGE}{TOOLS}/python /lab/jobs/fund_check.py {{{{ ds }}}}",
        append_env=True,
    )
    charges = BashOperator(
        task_id="ongoing_charges",
        bash_command=wrapped(
            "ongoing-charges",
            [f"{DB}.prices", f"{DB}.share_classes"],
            [f"{DB}.ongoing_charges"],
            f"{TOOLS}/python /lab/jobs/fund_charges.py",
        ),
        append_env=True,
    )
    reports = BashOperator(
        task_id="kid_reports",
        bash_command=wrapped(
            "kid-reports",
            [f"{DB}.ongoing_charges"],
            [f"s3://funds/reports/kid/{fund}.pdf" for fund in FUNDS],
            f"{TOOLS}/python /lab/jobs/fund_reports.py",
        ),
        append_env=True,
    )

    deliver >> ingest >> check >> charges >> reports
