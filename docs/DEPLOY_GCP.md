# Deploying to Google Cloud Run

This demo Streamlit UI can run on Cloud Run with artifacts stored in Cloud Storage.

## Prerequisites
- Google Cloud project with billing enabled.
- `gcloud` CLI authenticated to your project.
- A Cloud Storage bucket for artifacts, e.g. `gs://my-data-agents/artifacts`.
- Service account with Storage Object Admin on the bucket and Cloud Run Invoker for access.

## Bucket setup
1. Create the bucket (skip if it already exists):
   ```bash
   gsutil mb -l us-central1 gs://my-data-agents
   ```
2. Optional: create a prefix for artifacts:
   ```bash
   gsutil cp /dev/null gs://my-data-agents/artifacts/.keep
   ```

## Build and deploy
1. Build the container:
   ```bash
   gcloud builds submit --tag gcr.io/$(gcloud config get-value project)/data-agents
   ```
2. Deploy to Cloud Run (Streamlit binds to `$PORT` automatically):
   ```bash
   gcloud run deploy data-agents \
     --image gcr.io/$(gcloud config get-value project)/data-agents \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars ARTIFACTS_ROOT=gs://my-data-agents/artifacts
   ```

## Runtime environment
- `ARTIFACTS_ROOT` is required for Cloud Storage; use a `gs://bucket/prefix`.
- Cloud Run sets `$PORT`; the container binds to `0.0.0.0`.
- The application only needs Storage permissions; avoid broad roles. A minimal service account policy:
  - `roles/storage.objectAdmin` on your artifacts bucket.
  - `roles/run.invoker` for callers (if private).

## Notes
- Uploaded files are written to the artifact store; no local disk is required beyond `/tmp`.
- To rotate buckets, redeploy with a different `ARTIFACTS_ROOT`.
- The same image runs locally with a local artifacts folder:
  ```bash
  ARTIFACTS_ROOT=./artifacts streamlit run demos/streamlit_app.py
  ```
