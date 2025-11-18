# TransitX — Deployment & Inference Architecture (Local → Docker → Azure Container Apps)

TransitX exposes two kinds of model inference:

###  **1. Streaming / live inference** (FastAPI `app.py`)  
Predicts TTC transit delays for **past or future datetimes** using:
- Historical weather (Open-Meteo archive)
- Forecasted weather (Open-Meteo forecast)
- On-the-fly feature generation

###  **2. Batch inference** (`predict.py`)  
Predicts delays for large datasets (e.g., `transit_features.csv`) and uploads results to Azure Blob Storage.

This document describes how the API and batch inference were:

**Developed locally → Dockerized → Verified → Deployed to Azure Container Apps (ACA).**

---

## 1. Local FastAPI Development (`app.py`)

The primary inference API is implemented in:

`deployment/app.py`

It supports:

###  **Streaming predictions**
```python
POST /predict
```

Takes:
- date
- time
- route
- direction
- location
- incident
- min_gap

Then internally:
1. Extracts timestamp → hour/month/dayofweek
2. Fetches historical or forecast weather
3. Encodes categorical inputs
4. Loads ML models (xgb_regressor.pkl, encoders.pkl)
5. Generates:
    - delay minutes
    - is_delayed (boolean)
    - weather interpretation
    - human-readable summary

###  **Health Check**
```bash
GET /health
```

### **Docs**
```bash
GET /docs
```

### **Local development command:**
```bash
uvicorn deployment.app:app --reload
```

This validated:
- API input validation (Pydantic)
- Schema checking
- Feature transformation
- Weather API integration
- Model prediction consistency
- Summary text generation

---

## 2. Streaming Inference Logic (Inside `app.py`)

`app.py` performs feature engineering at inference time, including:

### **✔ Time features:**
- Hour
- Month
- Dayofweek
- Rush hour flag
- Weekend flag

### **✔ Weather features:**
- Temperature reading
- Rain intensity
- Temperature category
- Rain category

### **✔ Encoding:**
`encoders.pkl` is loaded and used to apply label encodings consistently.

### **✔ Regression model:**
`xgb_regressor.pkl` → predicts **delay_minutes**

---

## 3. Batch Inference (`predict.py`)

Batch inference is implemented in:

```bash
src/models/predict.py
```

### **Reads Input**:
```bash
data/model_input/transit_features.csv
```

### **Loads both models:**
- `xgb_classifier.pkl`
- `xgb_regressor.pkl`

### **Creates predictions:**
- pred_delay_minutes
- pred_is_delayed

### **Saves output locally:**
```bash
data/predictions/transit_predictions.csv
```

### **Uploads batch predictions to Azure Blob (predictions container):**
Uses:
```python
BlobServiceClient.from_connection_string(...)
```
### **Batch mode is used for:**
- Offline evaluation
- Reporting
- Bulk prediction jobs
- Airflow-triggered inference

---

## 4. Local Dockerization
`app.py` + models + encoders + FastAPI were packaged into a Docker image using:
```bash
deployment/Dockerfile
```
**Build:**
```bash
docker build -t transitx-api -f deployment/Dockerfile .
```
**Run locally:**
```bash
docker run -p 8000:8000 transitx-api
```

Validated:
- Model loading inside container
- Weather API communication
- Endpoints: `/predict`, `/health`, `/docs`
- Streaming inference in container
- Correct behavior for both past and future dates

---

## 5. Push to DockerHub
The verified image was tagged and pushed:
```bash
docker tag transitx-api jaynid00/transitx-api:latest
docker push jaynid00/transitx-api:latest
```

This image is used directly by Azure.

---

## 6. Deployment to Azure Container Apps (ACA)

The final model was deployed using the **Azure Portal** (GUI):

### Steps Performed:
1. **Create ACA environment** (`transitx-api`)
- Region: Canada Central
- Log analytics: (optional)

2. **Create Container App** (`transitx-api-app`)
- Source: **DockerHub**
- Image: `jaynid00/transitx-api:latest`
- Exposed port: 8000
- Ingress: Enabled (External)
- Environment Variables:
    - `AZ_STORAGE_CONNECTION_STRING=...`

3. **Deploy container**

4. **Verified endpoint via**:
```bash
https://<app-name>.<region>.azurecontainerapps.io/docs
```

---

## 7. Updating the Deployment
To update prediction logic or models:

### Step 1: Rebuild image
```bash
docker build -t transitx-api .
```
### Step 2: Push to DockerHub
```bash
docker push jaynid00/transitx-api:latest
```
### Step 3: Update ACA revision

(Portal → "Create Revision" OR via CLI)

Azure Container Apps automatically:
- Creates a new revision
- Keeps the old revision for rollback

---

## 8. Logs & Monitoring
### View ACA logs:

Azure Portal → Container Apps → Logs
### Or via CLI:
```bash
az containerapp logs show \
  --name transitx-api-app \
  --resource-group <RESOURCE_GROUP>
  ```

  ---

## 9. Summary of Deployment Flow

> **Deployment Flow**
>
> Local FastAPI (`app.py`)  
> ↓  
> Local Streaming Inference Testing  
> ↓  
> Local Batch Inference (`predict.py`)  
> ↓  
> Docker Image Built & Tested Locally  
> ↓  
> DockerHub Push (`jaynid00/transitx-api`)  
> ↓  
> Azure Portal → Container App Deployment  
> ↓  
> **Public HTTPS Endpoint (`/docs`, `/predict`)**


This workflow demonstrates full MLOps readiness:
- API development
- Feature engineering at inference time
- Batch and streaming predictions
- Containerization
- Cloud deployment (ACA)
- Model updating through CI/CD
- Azure Blob integration
- Weather API real-time data use

This deployment architecture is production-aligned, cloud-native, and demonstrates strong MLOps engineering practices.

---
