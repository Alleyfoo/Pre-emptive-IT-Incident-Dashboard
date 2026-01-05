#!/usr/bin/env bash
set -euo pipefail

# Deploys the dashboard Cloud Run service with IAM auth (no public access).
# Required env vars:
#   PROJECT_ID, REGION, IMAGE (Artifact Registry URL), BUCKET, DASHBOARD_SA

: "${PROJECT_ID:?PROJECT_ID required}"
: "${REGION:?REGION required}"
: "${IMAGE:?IMAGE required}"
: "${BUCKET:?BUCKET required}"
: "${DASHBOARD_SA:?DASHBOARD_SA required (dashboard-sa@project.iam.gserviceaccount.com)}"

gcloud run deploy incident-dashboard \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${DASHBOARD_SA}" \
  --set-env-vars ARTIFACTS_ROOT="gs://${BUCKET}/artifacts" \
  --port 8080 \
  --no-allow-unauthenticated \
  --ingress internal-and-cloud-load-balancing

echo "Dashboard deployed with IAM auth. Grant run.invoker to intended users/groups."
