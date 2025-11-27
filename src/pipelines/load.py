import os
import pandas as pd 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO


load_dotenv()

# ---------- GCP Environment ---------- #
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET", "transitx_dataset")
BQ_TABLE = os.getenv("BQ_TABLE", "processed_data")

storage_client = storage.Client()
bq_client = bigquery.Client()
bucket = storage_client.bucket(GCP_BUCKET_NAME)


# -------- Download from Blob -------- #
def download_from_blob(blob_name:str):

    blob = bucket.blob(f"processed/{blob_name}")
    csv_str = blob.download_as_text()
    return pd.read_csv(StringIO(csv_str))



# ----- Load the CSV file from Blob to Azure SQL ------ #
def load_to_sql(blob_name:str, table_name:str):
    
    df = download_from_blob(blob_name)

    # Clean column names for BigQuery compatibility
    df.columns = (
        df.columns.str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace("(", "")
                .str.replace(")", "")
                .str.replace("°", "")
                .str.replace("/", "_")
                .str.replace("-", "_")
    )

    table_id = f"{bq_client.project}.{BQ_DATASET}.{table_name}"
    job = bq_client.load_table_from_dataframe(df, table_id, 
                                              job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    
    job.result()
    print("Data successfully loaded to BigQuery.")




if __name__ == "__main__":

    blob_name = "transit_transformed_data_2023_2024.csv"

    load_to_sql(blob_name, table_name="transit_delay_weather")


        


