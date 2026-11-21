"""ACME's daily ELT: load raw data into the warehouse, transform it there, export.

    load_raw ──> dbt_run ──┬──> daily_revenue
                           └──> newsletter_export
    fraud_pipelines

Each task reaches OpenLineage a different way, because a pilot's DAGs do:

- `load_raw` is Python. Its lineage is declared, as Airflow Assets on `inlets` and
  `outlets`, which is the only way a PythonOperator has any.
- `dbt_run` runs `dbt-ol`, which emits its own events and names this task's run as
  its parent through `OPENLINEAGE_PARENT_ID`, so dbt's models sit under the task.
- `daily_revenue` is SQL through `SQLExecuteQueryOperator`; the provider parses the
  SQL for its lineage, and nobody declares any.
- `newsletter_export` runs under `art30-emit`, the one task that emits its own
  purpose and legal basis.
- `fraud_pipelines` runs pipeline-runtime's three example pipelines, which emit the
  `processing` facet, the provenance facet and resolved classification as they run.
  It shares nothing with the other tasks, as a second team's pipelines would not.

The day's data comes from `generate.py`, chosen by the logical date, so
`airflow dags test acme_daily 2026-11-03` is always day 2.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

sys.path.insert(0, "/lab")
import generate

RAW = "/warehouse/raw"
TABLES = ("customers", "consent_events", "orders")
WAREHOUSE = "postgres://warehouse:5432/acme"
TOOLS = "/opt/tools/bin"
PARENT = (
    "OPENLINEAGE_PARENT_ID={{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }} "
    "OPENLINEAGE_ROOT_PARENT_ID="
    "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
)


def load_raw(logical_date: datetime, **_: object) -> None:
    """Write the day's raw files and load each into `raw.<table>`, replacing it."""
    day = (logical_date.date() - generate.START).days + 1
    generate.write(day, Path(RAW))
    hook = PostgresHook(postgres_conn_id="warehouse")
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("create schema if not exists raw")
        for table in TABLES:
            with open(f"{RAW}/{table}.csv", encoding="utf-8") as f:
                columns = f.readline().strip().split(",")
                cur.execute(f"drop table if exists raw.{table}")
                cur.execute(f"create table raw.{table} ({', '.join(f'{c} text' for c in columns)})")
                f.seek(0)
                # The Postgres provider 7 hands out psycopg 3 connections, whose
                # COPY is a context manager; psycopg 2's copy_expert is gone.
                with cur.copy(f"copy raw.{table} from stdin with csv header") as copy:
                    copy.write(f.read())
        conn.commit()


with DAG(
    dag_id="acme_daily",
    start_date=datetime(2026, 11, 2),
    # Every day is triggered for a chosen logical date. With "@daily", the
    # scheduler started a run of its own for today, loading day 20's data beside
    # the day the lab asked for.
    schedule=None,
    catchup=False,
    tags=["acme"],
):
    load = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw,
        inlets=[Asset(f"file://{RAW}/{t}.csv") for t in TABLES],
        outlets=[Asset(f"{WAREHOUSE}/raw/{t}") for t in TABLES],
    )

    transform = BashOperator(
        task_id="dbt_run",
        bash_command=(
            # dbt-ol starts `dbt` by name, so the tools venv goes first on PATH.
            f"{PARENT} PATH={TOOLS}:$PATH "
            "OPENLINEAGE_NAMESPACE=acme DBT_SEND_ANONYMOUS_USAGE_STATS=false "
            "DBT_TARGET_PATH=/warehouse/dbt/target DBT_LOG_PATH=/warehouse/dbt/logs "
            f"{TOOLS}/dbt-ol run --project-dir /lab/dbt --profiles-dir /lab/dbt"
        ),
        append_env=True,
    )

    revenue = SQLExecuteQueryOperator(
        task_id="daily_revenue",
        conn_id="warehouse",
        sql=[
            "create schema if not exists finance",
            "create table if not exists finance.daily_revenue "
            "(ordered_on date, invoices integer, revenue_eur numeric(12, 2))",
            "delete from finance.daily_revenue where ordered_on = '{{ ds }}'",
            "insert into finance.daily_revenue "
            "select ordered_on, count(*), sum(amount_eur) from marts.billing_invoices "
            "where ordered_on = '{{ ds }}' group by ordered_on",
        ],
    )

    export = BashOperator(
        task_id="newsletter_export",
        bash_command=(
            f"{TOOLS}/art30-emit --job acme.crm/newsletter-export "
            "--purpose direct-marketing --legal-basis consent "
            f"--reads {WAREHOUSE.rsplit('/', 1)[0]}/acme.marts.crm_consent_state "
            f"--reads {WAREHOUSE.rsplit('/', 1)[0]}/acme.marts.crm_customers "
            "--writes file:///warehouse/exports/newsletter_optins.csv "
            f"--tag {WAREHOUSE.rsplit('/', 1)[0]}/acme.marts.crm_customers:data_category=contact "
            f"-- {TOOLS}/python /lab/jobs/newsletter_export.py "
            "postgresql://acme:acme@warehouse:5432/acme /warehouse/exports/newsletter_optins.csv"
        ),
        append_env=True,
    )

    # A fresh directory per day: the demo refuses one holding files it did not write.
    # OPENLINEAGE_CONFIG reaches it from the environment, so its events go through
    # the same composite transport as every other task's (pipeline-runtime 0.2.12).
    fraud = BashOperator(
        task_id="fraud_pipelines",
        bash_command=(
            "rm -rf /warehouse/pipeline-runtime/{{ ds }} && "
            "/opt/pipeline-runtime/bin/pipeline-runtime demo /warehouse/pipeline-runtime/{{ ds }}"
        ),
        append_env=True,
    )

    load >> transform >> [revenue, export]
