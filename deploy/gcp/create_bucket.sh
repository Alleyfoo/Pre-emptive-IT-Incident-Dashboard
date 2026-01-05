#!/usr/bin/env bash
set -euo pipefail

# Creates a dedicated bucket for artifacts + snapshots with lifecycle retention.
# Required env vars: PROJECT_ID, BUCKET, REGION
# Optional: RETENTION_DAYS (default 7)

: "${PROJECT_ID:?PROJECT_ID required}"
: "${BUCKET:?BUCKET required}"
: "${REGION:?REGION required}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

gsutil mb -p "${PROJECT_ID}" -c standard -l "${REGION}" "gs://${BUCKET}"

cat > /tmp/lifecycle.json <<EOF
{"rule":[{"action":{"type":"Delete"},"condition":{"age":${RETENTION_DAYS}}}]}
EOF
gsutil lifecycle set /tmp/lifecycle.json "gs://${BUCKET}"
rm /tmp/lifecycle.json

echo "Bucket gs://${BUCKET} created with delete-after-${RETENTION_DAYS}-days lifecycle."
echo "Create prefixes: artifacts/ and snapshots/ (implicit on first write)."
