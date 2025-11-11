import os
import subprocess
import requests
import pandas as pd 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient
from io import StringIO

# ---- Ensures current public IP has access to Azure SQL ----- #

def ensure_azure_firewall_rule():
    try:
        # Get your public IP
        ip = requests.get("https://ifconfig.me").text.strip()
        server_name = os.getenv("AZ_SQL_SERVER_NAME")  
        rule_name = f"auto_rule_{ip.replace('.', '_')}"

        print(f"Ensuring firewall access for current IP: {ip}")

        # Run Azure CLI command to add rule (idempotent)
        subprocess.run([
            "az", "sql", "server", "firewall-rule", "create",
            "--name", rule_name,
            "--resource-group", os.getenv("AZ_RESOURCE_GROUP"),
            "--server", server_name,
            "--start-ip-address", ip,
            "--end-ip-address", ip
        ], check=False)
        print("Firewall rule ensured for current IP.")
    except Exception as e:
        print(f"Could not update firewall rule automatically: {e}")


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

    ensure_azure_firewall_rule()
    df= download_from_blob(blob_name)

    engine = connect_sql()
    print(f"Loading {len(df)} rows into table {table_name}")

    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False, chunksize=5000)

    print("Data successfully loaded to Azure SQL.")




if __name__ == "__main__":

    blob_name = "transit_transformed_data_2023_2024.csv"

    load_to_sql(blob_name, table_name="transit_delay_weather")


        


