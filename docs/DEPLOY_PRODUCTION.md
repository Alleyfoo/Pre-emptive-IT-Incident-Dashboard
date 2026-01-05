# Production deploy checklist (GCP)

## Service accounts (least privilege)

- Create two SAs:
  - `dashboard-sa`: `roles/storage.objectViewer` scoped to `gs://$BUCKET/artifacts/**`
  - `worker-sa`: `roles/storage.objectAdmin` scoped to the same prefix (read/write/delete for retention)
- Keep dashboard unauthenticated access disabled; require Cloud Run IAM or IAP.

## Build + push image

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export REPO=preemptive-it
export IMAGE=us-central1-docker.pkg.dev/$PROJECT_ID/$REPO/incident-dashboard:latest

gcloud artifacts repositories create $REPO --repository-format=docker --location=$REGION --description="Pre-emptive IT"
gcloud builds submit --tag $IMAGE .
```

## Bucket + lifecycle + prefixes

- Use a dedicated bucket (blast-radius isolation), e.g., `gs://preemptive-it-$PROJECT_ID`.
- Apply lifecycle: delete objects after N days (see `deploy/gcp/create_bucket.sh` for automation).
- Prefixes:
  - `artifacts/` (worker read/write/delete for retention; dashboard read-only)
  - `snapshots/` (worker read/list; worker must NOT delete here)

### IAM
- `dashboard-sa`: `roles/storage.objectViewer` on the bucket (or narrower if you split buckets)
- `worker-sa`: `roles/storage.objectAdmin` on artifacts prefix + `roles/storage.objectViewer` on snapshots
- Prefer a dedicated bucket if prefix scoping is too loose.

## Deploy dashboard (Cloud Run service, auth required)

```bash
export BUCKET=preemptive-it-artifacts-$PROJECT_ID
gcloud run deploy incident-dashboard \
  --image $IMAGE \
  --region $REGION \
  --service-account dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars ARTIFACTS_ROOT=gs://$BUCKET/artifacts \
  --port 8080 \
  --no-allow-unauthenticated \
  --ingress internal-and-cloud-load-balancing
# Optionally front with IAP/ALB; otherwise grant IAM roles/run.invoker to allowed users/groups.
```

## Deploy worker (Cloud Run job)

```bash
gcloud run jobs create incident-worker \
  --image $IMAGE \
  --region $REGION \
  --task-timeout 900s \
  --service-account worker-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars ARTIFACTS_ROOT=gs://$BUCKET/artifacts \
  --command sh \
  --args "-c","python -m tools.generate_ticket_scenarios --run-id run-$(date +%Y%m%d-%H%M) --artifacts-root gs://$BUCKET/artifacts \
&& python -m runtime.incident_flow --run-id run-$(date +%Y%m%d-%H%M) --artifacts-root gs://$BUCKET/artifacts"
```

Run on demand:

```bash
gcloud run jobs execute incident-worker --region $REGION
```

## Scheduler trigger (hourly example)

```bash
export SA=incident-scheduler@$PROJECT_ID.iam.gserviceaccount.com
gcloud iam service-accounts create incident-scheduler --display-name="Incident worker scheduler"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/run.invoker"

gcloud scheduler jobs create http incident-worker-hourly \
  --schedule="0 * * * *" \
  --uri="$(gcloud run jobs describe incident-worker --region $REGION --format='value(status.executionUri)')" \
  --http-method=POST \
  --oidc-service-account-email=$SA
```

## Artifacts and pointers

- Worker writes `artifacts/latest_run.txt` after successful completion.
- `run_status.json` under each run captures status/message/timestamps for dashboard display.
- Retention: code purges old runs locally; set a GCS lifecycle rule (e.g., delete objects after 2 days) for the bucket prefix.

## Ingesting real snapshots (GCS input mode)

- Collectors upload schema-compliant snapshots to `gs://$BUCKET/snapshots/<host_id>/snapshot-<ts>.json`.
- Run worker with `--snapshot-root gs://$BUCKET/snapshots --snapshot-prefix ""` to process all snapshots (or a subprefix).
- Tickets are optional; pipeline works with snapshots only.

## Smoke test in GCP

1) Upload 3–5 snapshots to `gs://$BUCKET/snapshots/...`.
2) Execute the worker job (with `--snapshot-root gs://$BUCKET/snapshots` if using raw snapshots).
3) Open dashboard (authenticated) and confirm it loads latest run and shows clusters/hosts.
