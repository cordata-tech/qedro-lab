"""Drive the lab: run a day of ACME's pipelines, then check qedro reads every source.

    docker compose run --rm lab day 1     # data for day 1, dbt-ol, the art30-emit job
    docker compose run --rm lab check     # every source read, and compared

Each emitter sends every event to every destination through the composite transport
in openlineage.yml. `check` then reads them back the way a pilot would — with qedro's
own reader for the directory, Marquez and the three buckets, and a consumer that
commits no offsets for Kafka — and fails unless every source holds the same events.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path

import fsspec

import generate

WAREHOUSE = Path("/warehouse")
EVENTS = Path("/events/directory")
BUCKET = "openlineage"

#: Each source as a pilot would pass it to qedro, beside the name the check prints.
SOURCES = {
    "directory": str(EVENTS),
    "marquez": "http://marquez:5000",
    "s3": f"s3://{BUCKET}/events/",
    "gcs": f"gs://{BUCKET}/events/",
    "azure": f"az://{BUCKET}/events/",
}


def _filesystems() -> dict[str, fsspec.AbstractFileSystem]:
    """The emulators' filesystems, opened with the options openlineage.yml gives the
    transports, so the bucket the emitters write to is the one created here."""
    return {
        "s3": fsspec.filesystem("s3", endpoint_url="http://s3:5000", key="lab", secret="lab"),
        "gcs": fsspec.filesystem(
            "gcs", endpoint_url="http://gcs:4443", token="anon", project="qedro-lab"
        ),
        "azure": fsspec.filesystem(
            "abfs", connection_string=os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        ),
    }


def setup() -> None:
    """Create the bucket in each emulator, and the directory; harmless when they exist."""
    EVENTS.mkdir(parents=True, exist_ok=True)
    for name, fs in _filesystems().items():
        if not fs.exists(BUCKET):
            fs.mkdir(BUCKET)
            print(f"  created {name} bucket {BUCKET}")


def _run(command: list[str], **env: str) -> None:
    print(f"  $ {' '.join(command)}", flush=True)
    subprocess.run(command, check=True, env=os.environ | env)


def _dataset(suffix: str) -> str:
    """The `namespace/name` dbt-ol gave the table ending in *suffix*, read from the
    events rather than assumed, so the export names the table dbt wrote."""
    for path in sorted(EVENTS.iterdir()):
        event = json.loads(path.read_text())
        for dataset in event.get("outputs", []):
            if dataset["name"].endswith(suffix):
                return f"{dataset['namespace']}/{dataset['name']}"
    raise SystemExit(f"no dbt output ending in {suffix} among the events")


def day(number: int) -> None:
    setup()
    generate.write(number, WAREHOUSE / "raw")
    print(f"  wrote day {number}'s raw data to {WAREHOUSE / 'raw'}")
    _run(
        ["dbt-ol", "run", "--project-dir", "/lab/dbt", "--profiles-dir", "/lab/dbt"],
        OPENLINEAGE_NAMESPACE="acme",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        # dbt 1.12 deprecates these in dbt_project.yml; /lab is mounted read-only.
        DBT_TARGET_PATH="/warehouse/dbt/target",
        DBT_LOG_PATH="/warehouse/dbt/logs",
    )
    consent, customers = _dataset("crm_consent_state"), _dataset("crm_customers")
    export = "file:///warehouse/exports/newsletter_optins.csv"
    _run(
        [
            "art30-emit",
            "--job", "acme.crm/newsletter-export",
            "--purpose", "direct-marketing",
            "--legal-basis", "consent",
            "--reads", consent,
            "--reads", customers,
            "--writes", export,
            "--tag", f"{customers}:data_category=contact",
            "--",
            "python", "/lab/jobs/newsletter_export.py",
            str(WAREHOUSE / "acme.duckdb"), "/warehouse/exports/newsletter_optins.csv",
        ]
    )  # fmt: skip


def _key(event: dict) -> tuple[str, str, str]:
    return (event["run"]["runId"], event["eventType"], event["eventTime"])


def _qedro(source: str) -> tuple[Counter[tuple[str, str, str]], str]:
    from qedro import sources

    events, report = sources.read(source)
    keys = Counter(
        (e.run.run_id, e.event_type, e.event_time.isoformat() if e.event_time else "")
        for e in events
    )
    return keys, f"{report.events} events, {report.skipped_records} skipped"


def _kafka() -> tuple[Counter[tuple[str, str, str]], str]:
    """Every event on the topic, read with a fresh group that never commits: reading
    must not move anyone's offsets, which is the read-only rule a reader would keep."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
            "group.id": f"qedro-lab-check-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    topic = os.environ["KAFKA_TOPIC"]
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
    raw = []
    while (message := consumer.poll(5)) is not None:
        if message.error() is None:
            raw.append(json.loads(message.value()))
    consumer.close()
    keys = Counter((r, t, _normalise(at)) for r, t, at in map(_key, raw))
    return keys, f"{len(raw)} messages"


def _normalise(at: str) -> str:
    from datetime import datetime

    return datetime.fromisoformat(at).isoformat()


def check() -> int:
    """Read every source and compare it with the directory. Exit 1 on any difference.

    Compared with counts, not as sets: a source holding one event twice — a backend
    that stored a retried POST again — has to fail here rather than match.
    """
    found: dict[str, Counter[tuple[str, str, str]]] = {}
    failed = False
    for name, source in [*SOURCES.items(), ("kafka", "kafka")]:
        try:
            keys, summary = _kafka() if name == "kafka" else _qedro(source)
        except Exception as error:
            print(f"  {name:<10} could not be read: {type(error).__name__}: {error}")
            failed = True
            continue
        found[name] = keys
        print(f"  {name:<10} {summary}")
    reference = found.get("directory", Counter())
    for name, keys in found.items():
        if keys != reference:
            failed = True
            print(
                f"  {name} differs from the directory: {(reference - keys).total()} missing, "
                f"{(keys - reference).total()} extra"
            )
    if not failed:
        print(f"  every source holds the same {reference.total()} events")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="lab")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="create the buckets")
    day_cmd = sub.add_parser("day", help="run one day of ACME's pipelines")
    day_cmd.add_argument("number", type=int)
    sub.add_parser("check", help="read every source and compare")
    args = parser.parse_args()
    if args.command == "setup":
        setup()
    elif args.command == "day":
        day(args.number)
    else:
        return check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
