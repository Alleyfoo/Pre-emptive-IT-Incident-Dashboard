# Cloud Run deployment (dashboard + worker)

This project ships with a Cloud Run-ready container. The same image runs both the Streamlit dashboard and the worker job that generates snapshots, detects incidents, and writes artifacts to GCS.

## Prereqs

- gcloud CLI authenticated and set to the target project.
- Artifact Registry or Container Registry enabled.
- A GCS bucket for artifacts (read-only for dashboard, read/write for worker).

## Create the artifact bucket

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export BUCKET=preemptive-it-artifacts-$PROJECT_ID
gsutil mb -c standard -l $REGION gs://$BUCKET
# Optional: auto-delete objects older than 2 days (tweak as needed)
gsutil lifecycle set <(echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":2}}]}') gs://$BUCKET
# Prefixes (implicit on first write):
#   artifacts/ (worker read/write/delete; dashboard read-only)
#   snapshots/ (worker read/list)
```

## Build and push the image

```bash
export IMAGE=us-central1-docker.pkg.dev/$PROJECT_ID/preemptive-it/incident-dashboard:latest
gcloud artifacts repositories create preemptive-it --repository-format=docker --location=$REGION --description="Pre-emptive IT dashboard"
gcloud builds submit --tag $IMAGE .
```

## Deploy the dashboard (service)

```bash
gcloud run deploy incident-dashboard \
  --image $IMAGE \
  --region $REGION \
  --service-account dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --no-allow-unauthenticated \
  --set-env-vars ARTIFACTS_ROOT=gs://$BUCKET/artifacts \
  --port 8080 \
  --ingress internal-and-cloud-load-balancing
```

The container binds to `$PORT`/`0.0.0.0` automatically; the env var points the UI at your artifact bucket.

## Deploy the worker (Cloud Run job)

Use the same image but override the command to run the generator + incident flow (and optional validation). The example uses a date-based run id.

```bash
RUN_ID=$(date +%Y%m%d)
gcloud run jobs create incident-worker \
  --image $IMAGE \
  --region $REGION \
  --task-timeout 900s \
  --service-account worker-sa@$PROJECT_ID.iam.gserviceaccount.com \
  --command sh \
  --args "-c","python -m tools.generate_ticket_scenarios --run-id $RUN_ID --artifacts-root gs://$BUCKET/artifacts \
&& python -m runtime.incident_flow --run-id $RUN_ID --artifacts-root gs://$BUCKET/artifacts \
&& python -m tools.validate --run-id $RUN_ID --artifacts-root gs://$BUCKET/artifacts" \
  --set-env-vars ARTIFACTS_ROOT=gs://$BUCKET/artifacts
```

Trigger it on demand:

```bash
gcloud run jobs execute incident-worker --region $REGION
```

## Schedule daily runs with Cloud Scheduler

Create a service account that can invoke the job and assign it the `Cloud Run Invoker` role. Then schedule executions:

```bash
export SA=incident-scheduler@$PROJECT_ID.iam.gserviceaccount.com
gcloud iam service-accounts create incident-scheduler --display-name="Incident worker scheduler"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/run.invoker"

gcloud scheduler jobs create http incident-worker-daily \
  --schedule="0 2 * * *" \
  --uri="$(gcloud run jobs describe incident-worker --region $REGION --format='value(status.executionUri)')" \
  --http-method=POST \
  --oidc-service-account-email=$SA
```

## Access control and retention

- Dashboard service account: grant read-only access to `gs://$BUCKET/artifacts/**`.
- Worker job account: grant read/write to the same prefix.
- Enforce retention by configuring a bucket lifecycle rule (e.g., delete objects older than 24h) unless runs are pinned.

## Local parity

`docker compose up --build` runs the same container locally: the worker generates synthetic data and the dashboard reads `/artifacts`. Override `ARTIFACTS_ROOT` to point at a GCS bucket to inspect remote runs locally.
