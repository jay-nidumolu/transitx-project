# TransitX — Deployment & Inference Architecture 

***(Local → Docker → Azure Container Apps → Railway)***

TransitX exposes two kinds of model inference:

##  1. Streaming / live inference** (FastAPI)  
Implemented in:

`deployment/app.py`

This endpoint predicts TTC transit delays for **past or future timestamps** using:

- Weather archive (Open-Meteo)
- Weather forecast (Open-Meteo)
- Label encoders (`encoders.pkl`)
- Regression model (`xgb_regressor.pkl`)
- On-the-fly feature engineering

### Main Endpoint
```http
POST /predict
```
Input JSON:
- date
- time
- route
- direction
- location
- incident
- min_gap

Output JSON:
- predicted_delay_minutes
- is_delayed
- weather_condition
- rain_condition
- summary

### Supporting Endpoints
```http
GET /health
GET /docs
```
### Run locally
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

Batch inference performs:
- Model loading (classifier + regressor)
- Delay-minute prediction
- Delay-class prediction
- (Optional) Upload to Azure Blob Storage

Used for:
- Offline scoring
- Bulk predictions
- Airflow automated jobs

---

## 3. Dockerization

TransitX’s API is containerized via:

`deployment/Dockerfile`

**Build**

`docker build -t transitx-api -f deployment/Dockerfile .`

**Run**

`docker run -p 8000:8000 transitx-api`

Validates:
- Weather API access inside container
- Model + encoder file availability
- Containerized /predict behaves correctly

---

## 4. DockerHub Publishing


Once validated locally:
```bash
docker tag transitx-api jaynid00/transitx-api:latest
docker push jaynid00/transitx-api:latest
```

This pushed image becomes the artifact for cloud deployment.

---

## 5. Original Cloud Deployment — Azure Container Apps (ACA)

Azure ACA was the initial production deployment path.

### Deployment Steps
1. Create ACA Environment
  - Region: Canada Central
2. Create Container App (transitx-api-app)
  - Source: DockerHub
  - Image: jaynid00/transitx-api:latest
  - Port: 8000
  - Ingress: External Enabled
  - Environment Variables:
    - `AZ_STORAGE_CONNECTION_STRING=...`
3. Deploy Container

**Azure Swagger URL (no longer active)**
```bash
https://transitx-api-app.jollystone-d7b68f23.canadacentral.azurecontainerapps.io/docs
```
ACA provided:
- Autoscaling
- Secure ingress
- Managed revisions

---

## 6. Current Live Deployment — Railway
After Azure subscription expiration, the API was redeployed to **Railway**.

**Current Production API Endpoint**
```arduino
https://transitx-project-production.up.railway.app/predict
```
Railway Advantages:
- Free-tier hosting
- Auto-deploys on GitHub push
- Scale-to-zero mode
- Simple log viewer

This ensures the inference service remains public and accessible.

---

## 7. Updating the Deployment

### 1. Rebuild container
```bash
docker build -t transitx-api .
```

### 2. Push new version
```bash
docker push jaynid00/transitx-api:latest
```

### 3. Cloud pulls latest image
- Azure ACA → Create new revision
- Railway → Auto-deploys automatically

---

## 8. Logs & Monitoring

### Azure ACA Logs

Azure Portal → Container Apps → Logs

Or via CLI:
```bash
az containerapp logs show \
  --name transitx-api-app \
  --resource-group transitx-rg
```

### Railway Logs
Railway Dashboard → Deployments → Logs

---

## 9. Summary: Deployment Flow (Local → Cloud)
```scss
Local Development (FastAPI)
        ↓
Local Testing (Uvicorn / Docker)
        ↓
Docker Build
        ↓
DockerHub Push (jaynid00/transitx-api)
        ↓
Cloud Deployment
   • Azure Container Apps (original)
   • Railway (current live API)
        ↓
Public Inference Endpoint (/predict)
```

This workflow demonstrates full MLOps maturity:
- Real-time + batch inference
- On-demand feature engineering
- Reproducible Docker builds
- Cloud-native deployment
- Model & pipeline versioning (DVC)
- Automated rollout through CI/CD

---