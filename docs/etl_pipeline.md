# TransitX — ETL Pipeline

The TransitX ETL system processes TTC transit delay data and weather signals into clean, feature-rich datasets that support machine learning and analytics.  

All ETL components are stored inside:
`src/pipelines`

The pipeline follows a 4-stage flow:

**Extract → Transform → Feature Engineering → Load (Azure SQL)**  

and produces training data stored in **Azure Blob → model-input**.

---

## 1. Extract — `extract.py`

### Tasks:
- Download TTC Bus Delay datasets (2023 & 2024)
- Download weather data from the Open-Meteo API
- Save files locally in `data/raw/`
- Upload all raw datasets to **Azure Blob → `raw` container**

### Output Location:
- `raw/`

    - `ttc_bus_delay_2023.csv`

    - `ttc_bus_delay_2024.csv`
- `weather/`

    - `weather_2023.csv`

    - `weather_2024.csv`

---

## 2. Transform — `transform.py`

### Tasks:
- Clean, standardize, and merge raw datasets  
- Fix timestamps and missing values  
- Produce a unified processed dataset  
- Upload processed dataset to **Azure Blob → `processed` container**

### Output Location (Cloud):
- `processed/`
    - `ttc_combined_processed.csv`

---

## 3. Feature Engineering — `feature_eng.py`

### Tasks:
- Download processed dataset from **Azure Blob → `processed`**
- Generate ML-ready features, including:
  - Hour, weekday, month extraction  
  - Route-based features  
  - Weather enrichment  
  - Categorical encodings  
  - Delay label creation (classification target)
- Save feature-engineered dataset locally to:
  - `data/model_input/features.csv`
- Upload ML-ready dataset to:
  - **Azure Blob → `model-input` container**

### Output Locations:
**Local:**
`data/model_input/transit_features.csv`

**Cloud:**
- `model-input/`
  - `transit_features.csv`

This dataset is used directly by the model training scripts.

---

## 4. Load to Azure SQL — `load.py`

### Tasks:
- Download processed dataset from **Azure Blob → `processed`**
- Insert processed rows into **Azure SQL Database**
- Designed for:
  - Analytics  
  - Monitoring  
  - Power BI dashboards  
  - Historical delay reporting

### Notes:
- `load.py` does **not** feed into ML training.
- It is part of the **analytics/BI workflow**.

---

## Machine Learning Input Source

The training scripts:

`models/xgb_classifier.py`  
`models/xgb_regressor.py`  

consume **feature-engineered data** from:

**Cloud:**  
`Azure Blob → model-input/`

**Local:**  
`data/model_input/transit_features.csv`

The processed dataset alone is *not sufficient* for model training — feature engineering is required.

---

## Orchestration Methods

### Manual Development Orchestration — `main.py`
Originally, the entire pipeline was orchestrated using:

```bash
python main.py
```

This runs :

**`extract → transform → feature_eng → load`**

Useful for local development and debugging.

---

### Airflow Orchestration – Full ML Pipeline

A production-ready Airflow DAG later replaced manual orchestration and now performs:

**`extract → transform → feature_eng → model_training → dvc push`**

This automates:
- Data ingestion
- Data cleaning
- Feature creation
- Model retraining
- MLflow logging
- Model versioning (DVC)

This turns TransitX into a **fully automated MLOps training pipeline.**

---

This ETL pipeline cleanly separates raw ingestion, transformation, feature engineering, SQL loading, and ML training, supporting both data science and analytics workflows.