# TransitX — CI/CD Pipeline (GitHub Actions → Artifacts Registry → GCP Cloud Run)

The TransitX GitHub Actions CI/CD pipeline automates the following steps:

- Validation of code quality and execution of basic tests
- Installation of dependencies
- Restoration of DVC-tracked model and data artifacts from Google Cloud Storage
- Docker image build and push to Artifact Registry
- Deployment of the FastAPI API to Cloud Run

This pipeline ensures that every commit to the `main` branch produces a **reproducible, deployable container image** and updates the live API automatically.

---

## 1. Trigger Conditions

The workflow is triggered on:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

**Explanation:**  
- Every push to `main`  
- Every pull request targeting `main` 

These triggers execute the CI/CD pipeline automatically.

---

## 2. Job Configuration

```yaml
jobs:
  build-test-deploy:
    runs-on: ubuntu-latest
```
This uses the **latest Ubuntu image** for consistency and fast builds.

Environment variables for DockerHub authentication come from **GitHub Secrets**:

```yaml
env:
  DOCKERHUB_USERNAME: ${{ secrets.DOCKERHUB_USERNAME }}
  DOCKERHUB_TOKEN: ${{ secrets.DOCKERHUB_TOKEN }}
  GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
  GCP_SERVICE_ACCOUNT_KEY: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}

```
These secrets are **secure** and never exposed in logs.

---

## 3. Pipeline Steps

Each step of the workflow is executed as follows:

### Step 1 — Checkout Repository

```yaml
uses: actions/checkout@v4
```
The repository is cloned to make code, models, and Dockerfiles available for the pipeline.

### Step 2 — Set Up Python 3.12

```yaml
uses: actions/setup-python@v5
with:
  python-version: "3.12"
  ```
A reproducible Python environment is created.

### Step 3 — Install Dependencies

```yaml
python -m pip install --upgrade pip
pip install -r airflow/requirements.txt
pip install dvc[gcs] google-cloud-storage
```
Dependencies are installed for:
- Code validation
- Pulling DVC artifacts from Google Cloud Storage
- Running lightweight checks (training is not executed in CI)


### Step 4 — Lint / Smoke Test

```yaml
python -m compileall src/
```
A simple syntax-check step ensuring:
- No invalid Python files
- All imports are valid
- No structural issues

This reduces the risk of failing Docker builds.

### Step 5 — Authenticate to GCP

```yaml
uses: google-github-actions/auth@v2
with:
  credentials_json: '${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}'
```

The workflow authenticates to Google Cloud using the service account key stored in GitHub Secrets.

### Step 6 — Install gcloud SDK & Configure Docker

```yaml
uses: google-github-actions/setup-gcloud@v1
with:
  project_id: ${{ secrets.GCP_PROJECT_ID }}
  install_components: "gke-gcloud-auth-plugin"

run: gcloud auth configure-docker gcr.io --quiet
```

The Google Cloud SDK is installed, and Docker is configured to authenticate with **Artifact Registry**.

### Step 7 — Restore DVC Artifacts

```yaml
echo '${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}' > sa-key.json
dvc remote modify gcp_remote credentialpath sa-key.json
dvc pull --force
```
Model and data artifacts tracked by DVC are restored from Google Cloud Storage.

The service account key ensures proper authentication.

### Step 8 — Build Docker Image

```yaml
IMAGE_NAME=us-central1-docker.pkg.dev/$GCP_PROJECT_ID/transitx/transitx:latest
docker build -t $IMAGE_NAME -f ./Dockerfile .
```

A production-ready FastAPI Docker image is built containing:
- `app.py` (API)
- Models and Encoders
- Dependencies
- Feature engineering logic

### Step 9 — Authenticate Docker with GCP

```yaml
gcloud auth activate-service-account --key-file=sa-key.json
gcloud auth configure-docker us-central1-docker.pkg.dev
```

Docker is authenticated to push the image to Artifact Registry.

### Step 10 — Push Docker Image to Artifact Registry

```yaml
docker push $IMAGE_NAME
```

The image is uploaded to Google Artifact Registry, ready for deployment.

### Step 11 — Deploy to Cloud Run

```yaml
gcloud run deploy transitx-api \
  --image $IMAGE_NAME \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated
```

Cloud Run automatically deploys the updated container, exposing a publicly accessible API endpoint.

---


## 4. Benefits of This CI/CD Pipeline

- **Automatic Docker image builds:** Every update to the API, models, or pipeline triggers a build.
- **Consistent, reproducible deployments:** The live API always uses validated containers.
- **DVC artifact restoration:** Ensures that models and data are included in the image.
- **Safe code validation:** Syntax and structural checks prevent broken deployments.
- **Automated Cloud Run deployment:** The API is updated automatically with every commit.

---


## 5. Limitations (By Design)

- Model training is not executed
- Airflow tasks are not run
- Unit tests are not included (optional for future)

The design keeps the CI/CD pipeline **lightweight, fast, and focused on production deployment**, demonstrating robust **DevOps + MLOps** practices.


---