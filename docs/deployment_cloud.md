# TransitX — Deployment & Inference Architecture 

***(Local → Docker → Azure Container Apps → Railway → Google Cloud Run)***

TransitX delivers both real-time and batch inference for predicting TTC transit delays. The system evolved through multiple deployment environments—first Azure Container Apps, then Railway, and now fully migrated to Google Cloud Platform (GCP) using Cloud Run, Artifact Registry, and GCS for model storage.

##  1. Streaming / live inference** (FastAPI)  
Implemented in:

`deployment/app.py`

The API serves live predictions for **past or future timestamps** by dynamically fetching weather data and generating features on the fly.

### Core Input Features:
- date
- time
- route
- direction
- location
- incident
- min_gap

Using
- Weather archive (Open-Meteo)
- Weather forecast (Open-Meteo)
- Label encoders (`encoders.pkl`)
- Regression model (`xgb_regressor.pkl`)
- On-the-fly feature engineering

### Main Endpoint
```http
POST /predict
```

### Supporting Endpoints
```http
GET /health
GET /docs
```

### Local Run
```bash
uvicorn deployment.app:app --reload
```
Validates:
- Pydantic request schema
- Time/weather feature generation
- Model + encoder loading
- Summary explanation logic

---

## 2. Batch Inference (`predict.py`)

Implemented in:

`src/models/predict.py`

**Input**

`data/model_input/transit_features.csv`

**Output**

`data/predictions/transit_predictions.csv`

Performs:
- Encoder + Model loading (classifier + regressor)
- Delay-minute prediction
- Delay-class prediction
- Upload to Cloud Storage (initially Azure; now GCS-adapted)

Used for:
- Airflow jobs
- Scheduled offline inference
- Backfilling predictions

---

## 3. Dockerization

API is containerized using:

`Dockerfile`

**Build**

`docker build -t transitx-api -f Dockerfile .`

**Run**

`docker run -p 8080:8000 transitx-api`

Validates:
- Weather API access inside container
- Model + encoder file availability
- `/predict` behaves identically inside Docker

---

## 4. Initial Cloud Deployment — Azure Container Apps (Historic)

The first production deployment used **Azure Container Apps (ACA)**.

### Historic Deployment Steps
- ACA environment in Canada Central
- Container App sourced from DockerHub
- Image: `jaynid00/transitx-api:latest`
- External ingress enabled
- Environment variable for Azure Storage

**Historic Azure Endpoint (now inactive)**
```bash
https://transitx-api-app.jollystone-d7b68f23.canadacentral.azurecontainerapps.io/docs
```
ACA provided autoscaling, revisions, and secure ingress.

This stage was discontinued after the Azure subscription expired.

---

## 5. Temporary Deployment — Railway (Historic)
After Azure access ended, the project was temporarily deployed on **Railway**.

**Temporary Production URL**
```arduino
https://transitx-project-production.up.railway.app/predict
```

Railway handled:
- Auto-deploys from GitHub
- Free-tier hosting
- Scale-to-zero

This setup was later phased out due to the same free-tier limitations as Azure.

---

## 6. Current & Final Deployment — Google Cloud Run

The entire system has now been migrated to **Google Cloud**.

### 1. Components

**Artifact Registry**

Stores container images created via CI/CD.

**Google Cloud Storage (GCS)**

Stores:
- Trained models
- Encoders
- DVC-tracked artifacts

**Cloud Run**

Hosts the FastAPI inference service:
- Fully serverless
- Autoscaling
- Per-request billing
- Secure HTTPS endpoint

---

## 7. CI/CD Deployment Pipeline (GitHub Actions → Cloud Run)

The workflow performs:
1. Install dependencies
2. Authenticate to GCP using workload identity federation
3. Download DVC artifacts from GCS
4. Build Docker image
5. Push image to Artifact Registry
6. Deploy automatically to Cloud Run

This creates a **zero-touch deployment pipeline.**

---

## 8. Updating the Deployment

**Local Build**
```bash
docker build -t transitx-api .
```

**Push to Artifact Registry**

Occurs automatically via CI/CD.

**Cloud Run Deployment**

Triggered by GitHub Actions on push.

Cloud Run replaces the previous revision and switches traffic automatically.

---

## 9. Monitoring & Logs

### Cloud Run Logs

Accessible through:
- Google Cloud Console → Cloud Run → Logs
- Cloud Logging Queries

Provides:
- API request logs
- Prediction errors
- Container debug logs

### Artifact Registry
Used for image version tracking.

### GCS Logging
Tracks model + encoder retrieval events.

---

## 10. Summary: Entire Deployment Evolution
```java
Local Development (FastAPI)
        ↓
Local Docker Testing
        ↓
Docker Image Build
        ↓
Azure Container Apps (initial deployment)
        ↓
Railway (temporary deployment)
        ↓
Google Cloud Platform (final migration)
   • Artifact Registry
   • Google Cloud Storage
   • Cloud Run
        ↓
Public Inference Endpoint (/predict)
```

The Google Cloud Run deployment now serves as the stable, fully managed production setup and represents the final state of the migration.

---