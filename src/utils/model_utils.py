import os
from dotenv import load_dotenv
import pandas as pd
import mlflow
from azure.storage.blob import BlobServiceClient

load_dotenv()

# -- Loading The Data -- #
def load_data():
    path = "data/model_input/transit_features.csv"
    if not os.path.exists(path):
        raise FileNotFoundError("Run feature_eng.py before training")
    df = pd.read_csv(path)

    return df

# -- Upload model to Azure Blob -- #
def upload_to_blob(local_path, blob_name):
    conn_str = os.getenv("AZ_STORAGE_CONNECTION_STRING")
    svc = BlobServiceClient.from_connection_string(conn_str)
    container = svc.get_container_client(os.getenv("MODEL_CONTAINER", "models"))

    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
        print(f"Uploaded model -> Azure Blob: models/{blob_name}")

# -- MLFLOW Helper -- #
def mlflow_starter(experiment_name):
    mlflow.set_experiment(experiment_name)
    return mlflow.start_run()
