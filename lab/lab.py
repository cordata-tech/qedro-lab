"""Create the buckets, and check qedro reads the same events from every source.

    docker compose run --rm airflow day 1   # a day of ACME's DAG, run by Airflow
    docker compose run --rm lab check       # every source read, and compared

Each emitter sends every event to every destination through the composite transport
in openlineage.yml. `check` then reads them back the way a pilot would — with qedro's
own reader for the directory, Marquez and the three buckets, and a consumer that
commits no offsets for Kafka — and fails unless every source holds the same events.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections import Counter
from pathlib import Path

import fsspec

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
    sub.add_parser("check", help="read every source and compare")
    args = parser.parse_args()
    if args.command == "setup":
        setup()
    else:
        return check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
