#!/usr/bin/env bash
# Run one day of ACME's DAG: `day.sh 3` is the logical date 2026-11-04.
#
# `airflow dags test` runs the DAG in this process against a fresh SQLite
# metadata database, with no scheduler, which keeps a day reproducible. It emits
# the DAG run's COMPLETE but not its START, as qedro's Airflow fixture notes.
set -euo pipefail
[ "${1:-}" = day ] && shift   # `compose run airflow day 3` passes the word too
day="${1:?usage: day.sh [day] <number, 1 is 2026-11-02>}"
date=$(python -c "import datetime as d; print(d.date(2026, 11, 2) + d.timedelta(days=$day - 1))")
# The buckets, created here rather than by a one-shot service: `up --wait`
# counts a container that exited, even with 0, as a failure.
python /lab/lab.py setup
airflow db migrate > /dev/null
airflow dags test acme_daily "$date"

# `dags test` exits 0 when the run fails, so the outcome is read back instead.
state=$(airflow dags state acme_daily "$date" | tail -1)
echo "acme_daily $date: $state"
[ "$state" = success ]
