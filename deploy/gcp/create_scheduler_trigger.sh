#!/usr/bin/env bash
set -euo pipefail

# Creates a Cloud Scheduler trigger for the worker job.
# Required env vars: PROJECT_ID, REGION, SCHEDULER_SA
# Optional env vars: CRON (default "0 * * * *"), TIMEZONE (default "Etc/UTC")

: "${PROJECT_ID:?PROJECT_ID required}"
: "${REGION:?REGION required}"
: "${SCHEDULER_SA:?SCHEDULER_SA required}"
CRON="${CRON:-0 * * * *}"
TIMEZONE="${TIMEZONE:-Etc/UTC}"

EXEC_URI=$(gcloud run jobs describe incident-worker --region "${REGION}" --format='value(status.executionUri)')

gcloud scheduler jobs create http incident-worker-schedule \
  --schedule="${CRON}" \
  --time-zone="${TIMEZONE}" \
  --uri="${EXEC_URI}" \
  --http-method=POST \
  --oidc-service-account-email="${SCHEDULER_SA}"

echo "Scheduler trigger created for incident-worker with cron ${CRON} (${TIMEZONE})."
