import os
import pandas as pd 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from src.utils.firewall_helper import ensure_firewall_access
from io import StringIO


load_dotenv()

def connect_sql():
    conn_str = os.getenv("AZ_SQL_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("AZ_SQL_CONNECTION_STRING is missing in .env")
    engine= create_engine(conn_str)
    print("Connected to Azure SQL Database.")
    return engine


# -------- Download from Blob -------- #
def download_from_blob(blob_name:str, container_name="processed"):

    conn_str = os.getenv("AZ_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("AZ_STORAGE_CONNECTION_STRING missing in .env")
    
    svc = BlobServiceClient.from_connection_string(conn_str)
    container = svc.get_container_client(container_name)
    blob = container.download_blob(blob_name)

    csv_str = blob.readall().decode("utf-8")

    return pd.read_csv(StringIO(csv_str))



# ----- Load the CSV file from Blob to Azure SQL ------ #
def load_to_sql(blob_name:str, table_name:str):

    ensure_firewall_access()
    df= download_from_blob(blob_name)

    engine = connect_sql()
    print(f"Loading {len(df)} rows into table {table_name}")

    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False, chunksize=5000)

    print("Data successfully loaded to Azure SQL.")




if __name__ == "__main__":

    blob_name = "transit_transformed_data_2023_2024.csv"

    load_to_sql(blob_name, table_name="transit_delay_weather")


        


