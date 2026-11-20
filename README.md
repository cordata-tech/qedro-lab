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
| dbt-core with dbt-duckdb, through `dbt-ol` | 1.12.7, 1.11.1, openlineage-dbt 1.55.0 | ACME's CRM and billing models |
| art30-emit | 0.1.0 | a job that emits its own purpose and legal basis |
| qedro | 0.25.0 | the reader under test |

Every emitter uses one OpenLineage configuration, `lab/openlineage.yml`, whose
composite transport sends each event to all six destinations. That is the
arrangement qedro's [pilot guide](https://github.com/cordata-tech/qedro/blob/main/docs/pilot.md)
recommends for keeping a copy of events beside an existing backend, so the lab tests
the guide as well as the readers. MinIO is absent because it stopped publishing
community images; moto's server mode stands in for S3.

## Running it

```bash
docker compose up -d --wait            # the destinations
docker compose run --rm lab day 1      # day 1 of ACME's pipelines
docker compose run --rm lab day 2      # day 2: the same models over more rows
docker compose run --rm lab check      # every source read back and compared
docker compose down -v                 # everything, including the stored events
```

A day writes ACME's raw data for that day (`lab/generate.py`, the same rows for the
same day every time), runs the dbt models under `dbt-ol`, then runs the newsletter
export under `art30-emit`. `check` reads every source the way a pilot would — qedro's
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

- **gcsfs 2026.10 asks each bucket its type over gRPC**, unless
  `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=false`. fake-gcs-server does not answer gRPC in
  plain-HTTP mode, so every write waited 60 seconds for the probe to time out before
  gcsfs fell back. Real GCS answers it; the switch is in `lab/emulators.env`.
- **A slow Marquez stores retried events again.** The OpenLineage HTTP client gives
  up after 5 seconds and retries, and Marquez keeps every attempt. Under emulation on
  arm64 its POSTs took up to 9 seconds, and one day of events arrived in Marquez with
  three duplicates; `check` caught it once it compared counts rather than sets. The
  lab raises the timeout to 30 seconds. A busy production Marquez can do the same, so
  whether qedro should count an identical event once is a question for qedro.
- **qedro reaches every emulator through the environment alone**: `AWS_ENDPOINT_URL`
  for s3fs, `STORAGE_EMULATOR_HOST` for gcsfs, and `AZURE_STORAGE_CONNECTION_STRING`
  for adlfs. An emulator is not the cloud, so this tests qedro's reading code and
  fsspec, not a real bucket's permissions.

## What comes next

- Airflow 3 with its OpenLineage provider orchestrating the day, and a Spark job, so
  the parent-run shapes from qedro#8 come from a live run.
- An erasure on a schedule, so qedro's erasure projection has a real case.
- A `capture` command that writes a dated capture with a README naming every version,
  in the shape `tests/fixtures/` uses.
- CI that brings the stack up, runs a fortnight of days and the check, and runs qedro's
  projections over the result.
- A Kafka reader in qedro itself, tested against this topic.

## Licence

Apache-2.0, as qedro.
