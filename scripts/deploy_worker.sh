#!/usr/bin/env bash
set -euo pipefail

# Deploys the worker Cloud Run job.
# Required env vars:
#   PROJECT_ID, REGION, IMAGE, BUCKET, WORKER_SA

: "${PROJECT_ID:?PROJECT_ID required}"
: "${REGION:?REGION required}"
: "${IMAGE:?IMAGE required}"
: "${BUCKET:?BUCKET required}"
: "${WORKER_SA:?WORKER_SA required (worker-sa@project.iam.gserviceaccount.com)}"

gcloud run jobs create incident-worker \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --task-timeout 900s \
  --service-account "${WORKER_SA}" \
  --set-env-vars ARTIFACTS_ROOT="gs://${BUCKET}/artifacts" \
  --command sh \
  --args "-c","python -m runtime.incident_flow --artifacts-root gs://${BUCKET}/artifacts" \
  --max-retries 1

echo "Worker job created; run_id will auto-generate per execution."
