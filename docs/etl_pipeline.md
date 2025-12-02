# TransitX — ETL Pipeline

The TransitX ETL system processes TTC transit delay data and weather signals into clean, feature-rich datasets that support machine learning and analytics.  

All ETL components are stored inside:
`src/pipelines`

The pipeline follows a 4-stage flow:

**Extract → Transform → Feature Engineering → Load (BigQuery)**  

The final output is stored in **Google Cloud Storage (GCS) bucket `transitx-dvc-storage`** inside the `model_input/` folder.

---

## 1. Extract — `extract.py`

### Tasks:
- Download TTC Bus Delay datasets (2023 & 2024)
- Download weather data from the Open-Meteo API
- Save files locally in `data/raw/`
- Upload all raw datasets to **Google Cloud Storage (GCS)** under the `raw/` prefix

### Output Location:
- `transitx-dvc-storage/raw/`

    - `ttc_bus_delay_2023.csv`

    - `ttc_bus_delay_2024.csv`


    - `weather_2023.csv`

    - `weather_2024.csv`

---

## 2. Transform — `transform.py`

### Tasks:
- Clean, standardize, and merge raw datasets  
- Fix timestamps and missing values  
- Produce a unified processed dataset  
- Upload processed dataset to **GCS → `processed/`**

### Output Location (Cloud):
- `transitx-dvc-storage/processed/`
    - `ttc_combined_processed.csv`

---

## 3. Feature Engineering — `feature_eng.py`

### Tasks:
- Download processed dataset from **GCS → `processed/`**
- Generate ML-ready features, including:
  - Hour, weekday, month extraction  
  - Route-based features  
  - Weather enrichment  
  - Categorical encodings  
  - Delay label creation (classification target)
- Save feature-engineered dataset locally to:
  - `data/model_input/features.csv`
- Upload ML-ready dataset to:
  - **GCS → `model_input/`**

### Output Locations:
**Local:**
`data/model_input/transit_features.csv`

**Cloud (GCS):**
- `transitx-dvc-storage/model_input/`
  - `transit_features.csv`

This dataset is used directly by the model training scripts.

---

## 4. Load to BigQuery — `load.py`

### Tasks:
- Download processed dataset from **GCS → `processed/`**
- Insert processed rows into **BigQuery table** or analytical workloads.
- Designed for:
  - SQL-based Analytics  
  - Monitoring pipelines
  - Power BI dashboards  
  - Historical delay reporting

### Notes:
- `load.py` does **not** feed into ML training.
- It is part of the **analytics/BI workflow** only.

---

## Machine Learning Input Source

The training scripts:

`models/xgb_classifier.py`  
`models/xgb_regressor.py`  

consume **feature-engineered data** from:

**Cloud:**  
`transitx-dvc-storage/model_input/`

**Local:**  
`data/model_input/transit_features.csv`

The processed dataset alone is *not sufficient* for model training — feature engineering is required.

---

## Orchestration Methods

### Local Development Orchestration — `main.py`
The entire ETL flow may be executed locally using:

```bash
python main.py
```

This triggers:

**`extract → transform → feature_eng → load`**

Useful for local development and debugging.

---

### Airflow Orchestration – Full ML Pipeline Automation

A production-ready Airflow DAG provides automated orchestration.

The DAG executes:

**Extract → Transform → Feature Engineering → Model Training → dvc push**

This automated pipeline provides:
- Scheduled data ingestion
- Automated cleaning and feature creation
- Model retraining
- MLflow experiment tracking
- Versioning of datasets and models using DVC

This converts TransitX into a fully automated, reproducible MLOps system.

---

This ETL design provides a clear separation between ingestion, transformation, feature generation, SQL loading, and ML model preparation, supporting both data science and analytics use cases.