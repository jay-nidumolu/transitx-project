import os
from dotenv import load_dotenv
import pandas as pd
import mlflow
from azure.storage.blob import BlobServiceClient
from io import StringIO

load_dotenv()

conn_str = os.getenv("AZ_STORAGE_CONNECTION_STRING")
svc = BlobServiceClient.from_connection_string(conn_str)

# -- Loading The Data -- #
def load_data():
    blob_path = "transit_features.csv"
    container = svc.get_container_client(os.getenv("DATA_CONTAINER_MODEL_INPUT", "model-input"))
    try:
        blob_client = container.get_blob_client(blob_path)
        blob_data = blob_client.download_blob().readall()

        # Read CSV from blob content
        df = pd.read_csv(StringIO(blob_data.decode("utf-8")))
        print(f"[INFO] Loaded data from Azure Blob: model-input/{blob_path}")
        return df

    except Exception as e:
        raise FileNotFoundError(f"Could not load {blob_path} from Azure Blob: {e}")

# -- Upload model to Azure Blob -- #
def upload_to_blob(local_path, blob_name):
    container = svc.get_container_client(os.getenv("MODEL_CONTAINER", "models"))

    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
        print(f"Uploaded model -> Azure Blob: models/{blob_name}")

# -- MLFLOW Helper -- #
def mlflow_starter(experiment_name):
    mlflow.set_experiment(experiment_name)
    return mlflow.start_run()
