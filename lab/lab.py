"""Create the buckets, and check qedro reads the same events from every source.

    docker compose run --rm airflow day 1   # a day of ACME's DAG, run by Airflow
    docker compose run --rm lab check       # every source read, and compared
    docker compose run --rm lab summary     # the record's stable part, for CI
    docker compose run --rm lab capture airflow-3.4 --days 2   # a fixture for qedro
    docker compose run --rm lab records --day 1   # what a partner's nightly job writes

Each emitter sends every event to every destination through the composite transport
in openlineage.yml. `check` then reads them back the way a pilot would — with qedro's
own reader for the directory, Marquez and the three buckets, and a consumer that
commits no offsets for Kafka — and fails unless every source holds the same events.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import fsspec

# The environment overrides are for running `records` outside the container.
EVENTS = Path(os.environ.get("LAB_EVENTS", "/events/directory"))
RECORDS = Path(os.environ.get("LAB_RECORDS", "/records"))
CONFIG = Path(__file__).parent / "qedro.yaml"
REPO = Path("/repo")
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


#: What a record says that a second run of the same days must say again. Run ids,
#: event times and timestamps change every run, so they stay out.
STABLE = ("job", "domain", "reads", "writes", "classification", "unclassified", "gaps")


def _value(field: object) -> object:
    if isinstance(field, dict):
        return {k: field.get(k) for k in ("value", "provenance") if k in field}
    return field


def summary() -> dict:
    """The record qedro builds from the directory, reduced to what is stable.

    Compared in CI against `expected/summary.json`, so an emitter or qedro release
    that changes what the record says shows up as a diff to review.
    """
    out = subprocess.run(
        ["qedro", "ropa", str(EVENTS), "--config", str(CONFIG), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    record = json.loads(out)
    activities = [
        {
            **{k: a.get(k) for k in STABLE},
            "purpose": _value(a.get("purpose")),
            "legal_basis": _value(a.get("legal_basis")),
        }
        for a in record["activities"]
    ]
    return {
        "complete": record["complete"],
        "earned": record["earned"],
        "reasons": record["reasons"],
        "activities": sorted(activities, key=lambda a: a["job"]),
    }


def _versions() -> list[str]:
    """Every pinned image and package, read from the files that pin them."""
    pins: list[str] = []
    for line in (REPO / "compose.yaml").read_text().splitlines():
        match = re.match(r"\s+[\w-]+: &[\w-]+ (\S+)$", line)
        if match:
            pins.append(match.group(1))
    for name in ("airflow/Dockerfile", "airflow/requirements-airflow.txt",
                 "airflow/requirements-tools.txt", "airflow/requirements-pipeline-runtime.txt",
                 "lab/requirements.txt"):  # fmt: skip
        for line in (REPO / name).read_text().splitlines():
            line = line.strip()
            if line.startswith("RUN "):
                continue
            if line.startswith("FROM "):
                pins.append(line.removeprefix("FROM "))
            elif "==" in line and not line.startswith("#"):
                pins.append(line)
    return sorted(set(pins))


def capture(name: str, days: int) -> None:
    """Write the directory's events, ordered by time, with a README naming every
    version that produced them, in the shape qedro's `tests/fixtures/` uses."""
    events = [json.loads(p.read_text()) for p in sorted(EVENTS.iterdir())]
    events.sort(key=lambda e: (e["eventTime"], e["run"]["runId"], e["eventType"]))
    target = Path("/captures") / name
    target.mkdir(parents=True, exist_ok=False)
    (target / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    jobs = sorted({f"{e['job']['namespace']}/{e['job']['name']}" for e in events})
    pins = "\n".join(f"- `{p}`" for p in _versions())
    job_lines = "\n".join(f"- `{j}`" for j in jobs)
    (target / "README.md").write_text(
        f"Real OpenLineage from qedro-lab, not hand-written.\n\n"
        f"`events.jsonl` is {len(events)} events from {days} days of ACME's DAG, captured "
        f"on {date.today().isoformat()} with `lab capture {name} --days {days}` after "
        f"`scheduled.sh 1` to `scheduled.sh {days}` under Airflow's scheduler on a "
        f"fresh stack, between "
        f"{events[0]['eventTime']} and {events[-1]['eventTime']}. Every name in it is "
        f"fictitious.\n\n## Versions\n\n{pins}\n\n## Jobs\n\n{job_lines}\n"
    )
    print(f"  wrote {len(events)} events and a README to {target}")


#: The day `scheduled.sh 1` runs; day N is N - 1 days after it.
FIRST_DAY = date(2026, 11, 2)

#: What the nightly job writes, as a partner would point it: the published revenue
#: figure's provenance, and the erasure of the raw customer table.
PROVENANCE = "postgres://warehouse:5432/acme.finance.daily_revenue"
ERASED = "postgres://warehouse:5432/acme.raw.customers"


def _day(day: int) -> tuple[str, str, str]:
    """The logical date of day *day*, and the first and last event of its DAG run.

    Events carry the time they were sent, not the day the DAG ran for, so the day is
    found through the logical date Airflow's run facet names. Its first event stands
    for the morning a customer's erasure was requested; its last ends the window, so
    a day's records describe that day even when they are written again later.
    """
    logical = (FIRST_DAY + timedelta(days=day - 1)).isoformat()
    times = []
    for path in EVENTS.iterdir():
        event = json.loads(path.read_text())
        run = event["run"].get("facets", {}).get("airflow", {}).get("dagRun", {})
        if str(run.get("logical_date", "")).startswith(logical):
            times.append(event["eventTime"])
    if not times:
        raise SystemExit(f"day {day} ({logical}) has no events: run scheduled.sh {day} first")
    return logical, min(times), max(times)


def _write(*args: str) -> None:
    """Run qedro as a scheduled job would. A record that withholds the mark still
    exits 0; anything else that is not 0 is a failure the job should stop on."""
    result = subprocess.run(
        [os.environ.get("QEDRO", "qedro"), *args], capture_output=True, text=True
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout + result.stderr)
        raise SystemExit(f"qedro {args[0]} exited {result.returncode}")


def records(day: int) -> None:
    """Write every projection for *day* into records/<date>/, as JSON, PDF and
    workbook, and status.json against the day's lineage where anything is signed.

    Each day gets its own directory, so any day can be opened again. Sign-off files
    are carried forward from the latest earlier day, as they would stay in the
    repository a DPO signs in; the day's records are then built against them.
    """
    logical, morning, until = _day(day)
    target = RECORDS / logical
    target.mkdir(parents=True, exist_ok=True)
    days = sorted(p for p in RECORDS.iterdir() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))
    earlier = [p for p in days if p.name < logical]
    if earlier:
        for signoff in earlier[-1].glob("*.signoff.json"):
            if not (target / signoff.name).exists():
                shutil.copy2(signoff, target / signoff.name)
    source, window = str(EVENTS), ["--config", str(CONFIG), "--until", until]
    projections = {
        "ropa": ["ropa", source, *window],
        "quality": ["quality", source, *window],
        "provenance": ["provenance", source, *window, "--dataset", PROVENANCE],
        "erasure": ["erasure", source, *window, "--dataset", ERASED, "--since", morning],
    }
    for name, args in projections.items():
        signoff = target / f"{name}.signoff.json"
        if signoff.exists():
            args = [*args, "--signoff", str(signoff)]
        # qedro writes no PDF of the quality history yet (cordata-tech/qedro#37).
        for suffix in ("json", "xlsx") if name == "quality" else ("json", "pdf", "xlsx"):
            _write(*args, "--out", str(target / f"{name}.{suffix}"))
    if any(target.glob("*.signoff.json")):
        _write(
            "status", str(target), "--against", source, *window,
            "--format", "json", "--out", str(target / "status.json"),
        )
    # records/latest names the newest day, relative so it holds outside the container
    # too: a console pointed at it shows a new day on its next reload. Writing an
    # earlier day again leaves it where it is.
    latest = RECORDS / "latest"
    if latest.is_symlink():
        latest.unlink()
    latest.symlink_to(max(logical, days[-1].name))
    print(f"  wrote {logical}'s records to {target}")


def main() -> int:
    parser = argparse.ArgumentParser(prog="lab")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="create the buckets")
    sub.add_parser("check", help="read every source and compare")
    sub.add_parser("summary", help="the stable part of the record, as JSON")
    capture_cmd = sub.add_parser("capture", help="write the events as a fixture")
    capture_cmd.add_argument("name")
    capture_cmd.add_argument("--days", type=int, required=True)
    records_cmd = sub.add_parser("records", help="write a day's records, as a nightly job")
    records_cmd.add_argument("--day", type=int, required=True)
    args = parser.parse_args()
    if args.command == "records":
        records(args.day)
    elif args.command == "setup":
        setup()
    elif args.command == "summary":
        print(json.dumps(summary(), indent=2, sort_keys=True))
    elif args.command == "capture":
        capture(args.name, args.days)
    else:
        return check()
    return 0


if __name__ == "__main__":
    sys.exit(main())
