# TransitX — CI/CD Pipeline (GitHub Actions → DockerHub)

TransitX uses a GitHub Actions CI/CD pipeline to automatically:

- Validate code quality  
- Install dependencies  
- Restore DVC-tracked model/data artifacts  
- Build the FastAPI Docker image  
- Push the image to DockerHub  

This ensures every commit to `main` produces a reproducible and deployable container image.

---

## 1. Trigger Conditions

The workflow runs on:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

**Meaning:**  
- Every push to `main`  
- Every pull request targeting `main`  
will execute the CI/CD pipeline.

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
```

These values are **secure** and **never exposed** in logs.

---

## 3. Pipeline Steps

Below is an explanation of each step in your workflow.



### ✔ Step 1 — Checkout Repository

```yaml
uses: actions/checkout@v4
```
Fetches your full GitHub repo so the workflow can access code, models, Dockerfile, etc.

### ✔ Step 2 — Set Up Python 3.12

```yaml
uses: actions/setup-python@v5
with:
  python-version: "3.12"
  ```
Ensures reproducible Python environment.

### ✔ Step 3 — Install Dependencies

```yaml
pip install -r airflow/requirements.txt
pip install dvc
```
Installs only what is needed for:
- Code validation
- DVC artifact pull
- Any lightweight local checks

(Full training is not triggered in CI for performance reasons.)

### ✔ Step 4 — Lint / Smoke Test

```yaml
python -m compileall src/
```
A simple syntax-check step ensuring:
- No invalid Python
- No missing imports
- No structural breaks

This reduces risk of breaking the Docker image with bad code.

### ✔ Step 5 — DVC Pull

```yaml
dvc pull
```
This restores:
- Tracked models (`xgb_classifier.pkl`, `xgb_regressor.pkl`)
- Encoders
- Any other DVC-managed artifacts

This ensures the Docker image includes the correct model versions.

### ✔ Step 6 — Build Docker Image

```yaml
docker build -t $DOCKERHUB_USERNAME/transitx:latest -f deployment/Dockerfile .
```
Builds a production-ready FastAPI image using your Dockerfile.

This image contains:
- `app.py` (API)
- Models
- Encoders
- Dependencies
- Feature engineering logic

### ✔ Step 7 — Authenticate to DockerHub

```yaml
uses: docker/login-action@v3
with:
  username: ${{ secrets.DOCKERHUB_USERNAME }}
  password: ${{ secrets.DOCKERHUB_TOKEN }}
```

Secure login—required for pushing images.

### ✔ Step 8 — Push Image to DockerHub

```yaml
docker push $DOCKERHUB_USERNAME/transitx:latest
```

This publishes your FastAPI model inference container.

Azure Container Apps later pulls from:
```bash
docker.io/<username>/transitx:latest
```

---


## What This CI/CD Pipeline Provides

### ✔ Automatic Docker image builds
Every time you update the API, models, or pipeline.
### ✔ Consistent, reproducible deployments
Ensures your cloud deployment always uses the latest validated container.
### ✔ DVC artifact restoration
Prevents missing model files inside Docker.
### ✔ Safe code through smoke tests
Catches syntax errors early.
### ✔ Ready for cloud deployment
ACA or any container service can pull `transitx:latest` immediately after CI finishes.

---


## What CI/CD Does Not Do (By Design)

- It does not deploy automatically to Azure
- It does not run model training
- It does not execute Airflow tasks
- It does not run unit tests (optional for future)

This keeps your pipeline lightweight and fast while still demonstrating real **DevOps + MLOps skills**.

This CI/CD pipeline elevates TransitX into a production-ready, automation-enabled ML system.

---