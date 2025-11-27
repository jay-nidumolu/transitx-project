import os
from dotenv import load_dotenv
import pandas as pd
import mlflow
from google.cloud import storage
from io import StringIO

load_dotenv()

GCP_BUCKET = os.getenv("GCP_BUCKET_NAME")

storage_client = storage.Client()
bucket = storage_client.bucket(GCP_BUCKET)

# -- Loading The Data -- #
def load_data():
    blob_path = "model_input/transit_features.csv"
    try:
        blob = bucket.blob(blob_path)
        blob_data = blob.download_as_text()

        # Read CSV from blob content
        df = pd.read_csv(StringIO(blob_data))
        print(f"[INFO] Loaded data from Google Cloud Storage: {GCP_BUCKET}/{blob_path}")
        return df

    except Exception as e:
        raise FileNotFoundError(f"Could not load {blob_path} from Google Cloud Storage: {e}")

# -- Upload model to Google Cloud Storage -- #
def upload_to_blob(local_path, blob_name):
    blob = bucket.blob(f"models/{blob_name}")

    blob.upload_from_filename(local_path)
    print(f"Uploaded model -> Google Cloud Storage: models/{blob_name}")

# -- MLFLOW Helper -- #
def mlflow_starter(experiment_name):
    mlflow.set_experiment(experiment_name)
    return mlflow.start_run()
