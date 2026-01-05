#!/usr/bin/env bash
set -euo pipefail

# Creates a Cloud Scheduler job to trigger the worker Cloud Run job hourly.
# Required env vars:
#   PROJECT_ID, REGION, SCHEDULER_SA

: "${PROJECT_ID:?PROJECT_ID required}"
: "${REGION:?REGION required}"
: "${SCHEDULER_SA:?SCHEDULER_SA required (scheduler@project.iam.gserviceaccount.com)}"

EXEC_URI=$(gcloud run jobs describe incident-worker --region "${REGION}" --format='value(status.executionUri)')

gcloud scheduler jobs create http incident-worker-hourly \
  --schedule="0 * * * *" \
  --uri="${EXEC_URI}" \
  --http-method=POST \
  --oidc-service-account-email="${SCHEDULER_SA}"

echo "Scheduler job created to trigger incident-worker hourly."
