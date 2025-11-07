# TransitX - Transit Delay Prediction (Azure End-to-End)

**Goal:** Predict bus/train delays using historical transit, weather, and event signals.
**Scope:** Data engineering (Azure Blob), ML (sklearn/XGBoost), MLOps (MLflow, DVC), Deployment (Docker + Azure).


-----

## Tech Stack
- **Data Engineering:** Python, Azure Blob Storage 
- **ML / MLOps:** scikit-learn, XGBoost, MLflow, DVC, Azure ML 
- **API / Deployment:** FastAPI, Docker, Azure Container Registry (ACR), Azure Container Instances (ACI)
- **CI/CD:** GitHub Actions

-----


## Repository Layout
<pre>
transitx-project/
├─ data/ # (gitignored) local data cache
├─ notebooks/ # EDA, experiments
├─ src/
│ ├─ pipelines/ # extract.py, transform.py, load.py
│ ├─ models/ # train.py, predict.py
│ └─ utils/ # helpers (azure blob, logging)
├─ models/ # (gitignored) saved models
├─ deployment/ # FastAPI app, Dockerfile
├─ docs/ # diagrams, images
└─ .github/workflows/ # CI/CD

</pre>

-----

## Architecture Overview

```mermaid
flowchart LR
A[Transit APIs/Files] -->|Extract| B[Azure Blob (raw)]
C[Weather API] -->|Extract| B
B -->|Transform| D[Azure Blob (processed)]
D -->|Train| E[Azure ML + MLflow]
E -->|Package (Docker)| F[Azure Container Registry (ACR)]
F -->|Deploy| G[Azure Container Instances (FastAPI)]
G -->|Serve| H[Client / Streamlit Dashboard]

-----


## Local Setup

```bash
#create virtual environment
python3 -m venv .venv
source .venv/bin/activate

#install dependencies
pip install -r requirements.txt

#copy or edit environment variables
cp .env.example .env # ( if you have an example file)
#or manually fill .env with your Azure Details

