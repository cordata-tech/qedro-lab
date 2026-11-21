# qedro-lab

Every place [qedro](https://github.com/cordata-tech/qedro) reads OpenLineage from, run
on one machine with Docker Compose, and real emitters writing to all of them at once.
It exists so that qedro's readers are tested against the systems a pilot actually
runs rather than against mocks, and so that the captures in qedro's
`tests/fixtures/` can be produced again by anyone, from a pinned set of versions.

Everything in it is fictitious: ACME Finanz GmbH, its customers *Kundin 0001* and
onwards in Musterstadt, 00000, and addresses at `.example`, as in `qedro demo`.

## What runs

| Component | Version | What it stands for |
|---|---|---|
| Marquez, its database and web UI | 0.51.1, Postgres 16.16 | the OpenLineage backend qedro reads over HTTP |
| Apache Kafka | 4.4.0, single node | a topic the events also land on |
| moto | 5.2.5 | S3 |
| fake-gcs-server | 1.56.1 | Google Cloud Storage |
| Azurite | 3.38.0 | Azure Blob Storage |
| a directory, `events/directory` | | files on a disk |
| Postgres, the warehouse | 16.16 | where ACME transforms its data, standing in for Redshift |
| Apache Airflow with its OpenLineage provider | 3.4.0, provider 2.21.0 | ACME's daily DAG |
| dbt-core with dbt-postgres, through `dbt-ol` | 1.12.7, 1.11.0, openlineage-dbt 1.55.0 | ACME's CRM and billing models, run from the DAG |
| art30-emit | 0.1.0 | a task that emits its own purpose and legal basis |
| pipeline-runtime, in a venv of its own | 0.2.12 | a second team's pipelines, emitting the `processing` facet, provenance and classification |
| qedro | 0.25.0 | the reader under test |

Every emitter uses one OpenLineage configuration, `lab/openlineage.yml`, whose
composite transport sends each event to all six destinations. That is the
arrangement qedro's [pilot guide](https://github.com/cordata-tech/qedro/blob/main/docs/pilot.md)
recommends for keeping a copy of events beside an existing backend, so the lab tests
the guide as well as the readers. MinIO is absent because it stopped publishing
community images; moto's server mode stands in for S3.

## Running it

Under Airflow's scheduler, as production runs it, which is the mode CI uses:

```bash
docker compose --profile scheduler up -d --wait          # destinations, warehouse, Airflow
docker compose exec airflow-scheduler /opt/airflow/scheduled.sh 1   # trigger day 1, wait
docker compose exec airflow-scheduler /opt/airflow/scheduled.sh 2   # day 2: more rows
docker compose run --rm lab check                        # every source read back and compared
docker compose run --rm lab summary                      # what the record says, minus run ids
docker compose run --rm lab capture <name> --days 2      # the events as a fixture for qedro
docker compose --profile run --profile scheduler down -v # everything, including stored events
```

Airflow's UI is then at <http://127.0.0.1:8080>, with the password `airflow standalone`
prints in its log. A quicker mode runs each day in one process with `airflow dags
test`, which is how the lab started and is kept for fast iteration:

```bash
docker compose up -d --wait
docker compose run --rm airflow day 1
```

It differs from a scheduled run in two ways that matter to qedro, listed under the
findings below, so captures and CI use the scheduler. The reset needs both profiles:
without them Compose leaves those services' volumes in place, and the next day reads
the last run's files.

A day is one run of the DAG in `airflow/dags/acme_daily.py`, with `airflow dags test`
at the logical date for that day, `day 1` being 2026-11-02:

```
load_raw ──> dbt_run ──┬──> daily_revenue
                       └──> newsletter_export
```

Each task reaches OpenLineage a different way, because a pilot's DAGs do:

- `load_raw` writes the day's raw data (`lab/generate.py`, the same rows for the same
  day every time) and loads it into the warehouse. It is Python, so its lineage is
  declared, as Airflow Assets on `inlets` and `outlets`.
- `dbt_run` runs `dbt-ol`, which emits its own events and names the task's run as its
  parent through `OPENLINEAGE_PARENT_ID`, so the models sit under the task.
- `daily_revenue` is SQL through `SQLExecuteQueryOperator`, and the provider parses the
  SQL for its lineage; nobody declares any.
- `newsletter_export` runs under `art30-emit`, the one task emitting its own purpose and
  legal basis.

`check` reads every source the way a pilot would — qedro's
own reader for the directory, Marquez and the three buckets, and a Kafka consumer that
commits no offsets — and fails unless each holds the same events the same number of
times:

```
  directory  20 events, 0 skipped
  marquez    20 events, 0 skipped
  s3         20 events, 0 skipped
  gcs        20 events, 0 skipped
  azure      20 events, 0 skipped
  kafka      20 messages
  every source holds the same 20 events
```

To build a record from what the lab emitted:

```bash
docker compose run --rm --entrypoint qedro lab ropa /events/directory --config /lab/qedro.yaml
```

Marquez's web UI is at <http://127.0.0.1:3000>. Every port is bound to `127.0.0.1`,
because the emulators run with fixed, published credentials.

## What it has found so far

Each finding is fixed in the configuration, with its cause written beside the fix.

- **gcsfs 2026.10 asks each bucket its type over gRPC**, unless
  `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false`. fake-gcs-server does not answer gRPC in
  plain-HTTP mode, so every write waited 60 seconds for the probe to time out before
  gcsfs fell back. Real GCS answers it; the switch is in `lab/emulators.env`.
- **A slow Marquez stores retried events again.** The OpenLineage HTTP client gives
  up after 5 seconds and retries, and Marquez keeps every attempt. Under emulation on
  arm64 its POSTs took up to 36 seconds, and one day of events arrived in Marquez with
  three duplicates; `check` caught it once it compared counts rather than sets, and
  caught it again at a 30-second timeout. The lab allows 120 seconds. A busy production Marquez can do the same, so
  whether qedro should count an identical event once is cordata-tech/qedro#58.
- **qedro reaches every emulator through the environment alone**: `AWS_ENDPOINT_URL`
  for s3fs, `STORAGE_EMULATOR_HOST` for gcsfs, and `AZURE_STORAGE_CONNECTION_STRING`
  for adlfs. An emulator is not the cloud, so this tests qedro's reading code and
  fsspec, not a real bucket's permissions.

Running Airflow found twelve more, none of them qedro's:

- **A slow first destination costs the copies events.** The composite transport sends
  in the order listed, and Airflow's provider stops an emission after
  `[openlineage] execution_timeout`, 10 seconds by default. With Marquez listed first, a
  17-second POST was stopped after Marquez had stored a `START` and before any copy was
  written, and the directory never saw it. The copies are now listed first and the
  limit is 180 seconds. qedro's pilot guide lists the backend first, so it needs the
  same advice.
- **`airflow dags test` is not a scheduled run, in two ways qedro sees.** It emits the
  DAG run's `COMPLETE` but never its `START`. And it files hook-level lineage under
  the wrong task: each SQL statement a hook ran becomes a child job,
  `acme_daily.newsletter_export.hll_query.1` to `.4`, which are `daily_revenue`'s four
  statements filed under the task that ran next, with day 2's twice over. qedro then
  listed four activities with no datasets and no purpose, and left out
  `newsletter_export`, which those children made a parent. Under the scheduler, with
  each task in a process of its own, neither happens: the DAG run emits both events,
  no `hll_query` job appears, and `daily_revenue`'s own events carry its lineage,
  `marts.billing_invoices` → `finance.daily_revenue`. So this is the one-process test
  command rather than a provider bug, and captures come from the scheduler.
- **A task that succeeded has 20 seconds for its listeners.** Under the scheduler,
  Airflow keeps a successful task's process for `[core] task_success_overtime`, 20
  seconds by default, and then ends it. `daily_revenue`'s `COMPLETE` to six
  destinations took longer: the directory and two buckets received it, Azure, Kafka
  and Marquez did not, and the task's log stops at the start of the emission. The lab
  allows 180 seconds.
- **The standalone API server answers heartbeats slowly under load.** With every
  Airflow component in one container beside Marquez under Rosetta, heartbeat requests
  took 20 to 32 seconds, the server ended one task as no longer running, and the retry
  succeeded. The run's lineage then carries a `FAIL` and a second attempt, which is a
  real shape but a lab artefact here; a deployment with its own API server should not
  see it.
- **An emitter needs the packages its transports need.** pipeline-runtime 0.2.12
  honours `OPENLINEAGE_CONFIG` (pipeline-runtime#17), but its `run` extra installs
  `openlineage-python` without `fsspec` or `kafka`, so a composite with bucket
  children failed with *fsspec is not available*. Its venv here installs them
  beside it, and its README now says so (pipeline-runtime#18).
- **On Linux, Airflow cannot write a bind mount it does not own.** The image runs as
  uid 50000 and the runner's checkout belongs to the runner, so the first CI run
  failed with `Permission denied: '/events/directory'`; on macOS the file sharing
  hides ownership. Airflow runs as `${AIRFLOW_UID:-50000}:0`, as Airflow's own
  Compose file does, and CI writes its uid to `.env` first.
- **A DAG with a schedule runs on its own once unpaused.** With `schedule="@daily"`, the
  scheduler started a run for today beside the day the lab triggered, loading day 20's
  data. The DAG has no schedule; every day is triggered for its logical date.
- **`up --wait` fails on a one-shot service** that exited with 0, so the buckets are
  created at the start of each day instead.
- **A fresh Airflow container can outrun its own DAG import limit.** The first import
  discovers every provider, and while Marquez ran its migrations under emulation that
  took longer than `[core] dagbag_import_timeout`'s 30 seconds, so day 1 failed before
  any task ran. The limit is 180 seconds.

- **`airflow dags test` exits 0 when the DAG run fails.** `airflow/day.sh` reads the
  run's state back with `airflow dags state` and fails unless it is `success`; before
  that, a failed load reported success.
- **The Postgres provider 7 hands out psycopg 3 connections**, which have no
  `copy_expert`; the load uses psycopg 3's `cursor.copy`.
- **`dbt-ol` starts `dbt` by name**, so with dbt in a virtualenv of its own, as it is
  here to keep its dependencies away from Airflow's, that venv has to be first on
  `PATH`.
- **Marquez's first start on a fresh volume** runs its migrations under emulation and
  outlasted its healthcheck, failing `up --wait`; it has a five-minute start period.

## What comes next

- [#1](https://github.com/cordata-tech/qedro-lab/issues/1) warehouse shapes: Databricks
  through Spark with Delta, BigQuery and Snowflake.
- [#2](https://github.com/cordata-tech/qedro-lab/issues/2) captures for qedro's
  fixtures, CI over a simulated fortnight, and Airflow under its scheduler.
- [#3](https://github.com/cordata-tech/qedro-lab/issues/3) a scheduled erasure that
  leaves one descendant behind on purpose.
- A Kafka reader in qedro itself (cordata-tech/qedro#59), tested against this topic.

## Licence

Apache-2.0, as qedro.
