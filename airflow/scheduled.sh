#!/usr/bin/env bash
# Run one day of ACME's DAGs under the scheduler: `scheduled.sh 3` triggers the
# logical date 2026-11-04 for acme_daily and then fund_feed, and waits for each
# run to finish.
#
#   docker compose --profile scheduler up -d --wait
#   docker compose exec airflow-scheduler /opt/airflow/scheduled.sh 1
set -euo pipefail
[ "${1:-}" = day ] && shift
day="${1:?usage: scheduled.sh [day] <number, 1 is 2026-11-02>}"
date=$(python -c "import datetime as d; print(d.date(2026, 11, 2) + d.timedelta(days=$day - 1))")
python /lab/lab.py setup
for dag in acme_daily fund_feed; do
  airflow dags trigger "$dag" --logical-date "${date}T00:00:00+00:00" > /dev/null
  # Wait for the run to leave queued and running; 30 minutes is far past a slow day.
  for _ in $(seq 180); do
    state=$(airflow dags state "$dag" "${date}T00:00:00+00:00" | tail -1)
    case "$state" in
      success|failed) break ;;
    esac
    sleep 10
  done
  echo "$dag $date: $state"
  [ "$state" = success ]
done
