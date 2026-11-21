#!/usr/bin/env bash
# Run one day of ACME's DAG under the scheduler: `scheduled.sh 3` triggers the
# logical date 2026-11-04 and waits for the run to finish.
#
#   docker compose --profile scheduler up -d --wait
#   docker compose exec airflow-scheduler /opt/airflow/scheduled.sh 1
set -euo pipefail
[ "${1:-}" = day ] && shift
day="${1:?usage: scheduled.sh [day] <number, 1 is 2026-11-02>}"
date=$(python -c "import datetime as d; print(d.date(2026, 11, 2) + d.timedelta(days=$day - 1))")
python /lab/lab.py setup
airflow dags trigger acme_daily --logical-date "${date}T00:00:00+00:00" > /dev/null
# Wait for the run to leave queued and running; 30 minutes is far past a slow day.
for _ in $(seq 180); do
  state=$(airflow dags state acme_daily "${date}T00:00:00+00:00" | tail -1)
  case "$state" in
    success|failed) break ;;
  esac
  sleep 10
done
echo "acme_daily $date: $state"
[ "$state" = success ]
