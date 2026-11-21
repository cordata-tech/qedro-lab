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
| qedro | 0.25.0 | the reader under test |

Every emitter uses one OpenLineage configuration, `lab/openlineage.yml`, whose
composite transport sends each event to all six destinations. That is the
arrangement qedro's [pilot guide](https://github.com/cordata-tech/qedro/blob/main/docs/pilot.md)
recommends for keeping a copy of events beside an existing backend, so the lab tests
the guide as well as the readers. MinIO is absent because it stopped publishing
community images; moto's server mode stands in for S3.

## Running it

```bash
docker compose up -d --wait                  # the destinations and the warehouse
docker compose run --rm airflow day 1        # day 1 of ACME's DAG
docker compose run --rm airflow day 2        # day 2: the same DAG over more rows
docker compose run --rm lab check            # every source read back and compared
docker compose --profile run down -v         # everything, including stored events
```

The last line needs `--profile run`: without it Compose leaves the volumes of the
services under that profile in place, and the next day reads the last run's files.

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

Running Airflow found seven more, none of them qedro's:

- **A slow first destination costs the copies events.** The composite transport sends
  in the order listed, and Airflow's provider stops an emission after
  `[openlineage] execution_timeout`, 10 seconds by default. With Marquez listed first, a
  17-second POST was stopped after Marquez had stored a `START` and before any copy was
  written, and the directory never saw it. The copies are now listed first and the
  limit is 180 seconds. qedro's pilot guide lists the backend first, so it needs the
  same advice.
- **Hook-level lineage lands under the wrong task.** The provider emits each SQL
  statement a hook ran as a child job, `acme_daily.newsletter_export.hll_query.1`
  to `.4`, but these are `daily_revenue`'s four statements, filed under the next task,
  and day 2's appear twice. Under `airflow dags test` every task runs in one process,
  so the provider's collector of hook lineage seemingly is not cleared between tasks;
  whether a scheduled run does the same is part of
  [#2](https://github.com/cordata-tech/qedro-lab/issues/2). `daily_revenue`'s own
  events are right: the provider parsed its SQL into `marts.billing_invoices` →
  `finance.daily_revenue`.
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
- [#4](https://github.com/cordata-tech/qedro-lab/issues/4) pipeline-runtime as an
  emitter, once it honours `OPENLINEAGE_CONFIG`.
- A Kafka reader in qedro itself (cordata-tech/qedro#59), tested against this topic.

## Licence

Apache-2.0, as qedro.
