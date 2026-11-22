"""Check the day's prices, and report each check as OpenLineage evidence.

The checks are what such a service already runs. The lineage part is the block marked
below: about twenty lines with the OpenLineage client, which art30-emit cannot do for
a job, because a check's result is something only the job knows. qedro's assertion
history reads them, so *which checks ran on the inputs, and did they hold* is answered
from what ran rather than from a test plan.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, date, datetime

import psycopg

sys.path.insert(0, "/lab")
import funds


def checks(ds: str) -> list[tuple[str, str, bool]]:
    """(assertion, column, held) for the day's prices."""
    with psycopg.connect(funds.DSN) as conn:
        expected = {r[0] for r in conn.execute("select share_class from funds.share_classes")}
        rows = conn.execute("select share_class, price_date, nav from funds.prices").fetchall()
    priced = {r[0] for r in rows}
    return [
        ("every share class in the reference data has a price", "share_class", expected <= priced),
        ("every NAV is positive", "nav", all(r[2] > 0 for r in rows)),
        ("every price is for the reporting date", "price_date",
         all(r[1] == date.fromisoformat(ds) for r in rows)),
    ]  # fmt: skip


# --- The lineage: what a team adds to a check it already has. ---------------------

from openlineage.client import OpenLineageClient  # noqa: E402
from openlineage.client.event_v2 import InputDataset, Job, Run, RunEvent, RunState  # noqa: E402
from openlineage.client.facet_v2 import data_quality_assertions_dataset as dq  # noqa: E402
from openlineage.client.facet_v2 import parent_run  # noqa: E402


def _parent() -> dict:
    """The Airflow task that started this, as dbt-ol and art30-emit name it."""
    value = os.environ.get("OPENLINEAGE_PARENT_ID", "")
    if value.count("/") != 2:
        return {}
    namespace, job, run_id = value.split("/")
    return {
        "parent": parent_run.ParentRunFacet(
            run=parent_run.Run(runId=run_id), job=parent_run.Job(namespace=namespace, name=job)
        )
    }


def emit(results: list[tuple[str, str, bool]]) -> None:
    client = OpenLineageClient()
    run = Run(runId=str(uuid.uuid4()), facets=_parent())
    job = Job(namespace="acme.funds", name="prices-check")
    namespace, _, schema = funds.WAREHOUSE.rpartition("/")
    assertions = dq.DataQualityAssertionsDatasetFacet(
        assertions=[dq.Assertion(assertion=a, success=ok, column=c) for a, c, ok in results]
    )
    # The results go on COMPLETE alone, once they are known: on START as well, a
    # reader counting assertions per event sees every check twice.
    for state, checked in ((RunState.START, {}), (RunState.COMPLETE, assertions)):
        prices_facets = {"dataQualityAssertions": checked} if checked else {}
        inputs = [
            InputDataset(namespace=namespace, name=f"{schema}.prices", inputFacets=prices_facets),
            InputDataset(namespace=namespace, name=f"{schema}.share_classes"),
        ]
        client.emit(
            RunEvent(
                eventType=state,
                eventTime=datetime.now(UTC).isoformat(),
                run=run,
                job=job,
                inputs=inputs,
                producer="https://github.com/cordata-tech/qedro-lab",
            )
        )


# -----------------------------------------------------------------------------------


def main(ds: str) -> None:
    results = checks(ds)
    emit(results)
    for assertion, _, ok in results:
        print(f"{'held' if ok else 'FAILED'}: {assertion}")
    # A failed check is the exception a person approves; the run goes on with the
    # share classes it has, as a reporting service does, and the evidence says so.


if __name__ == "__main__":
    main(sys.argv[1])
