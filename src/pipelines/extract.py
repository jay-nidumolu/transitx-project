import os
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime

# ----- Load environment variables ----- #
load_dotenv()


# ----- Azure Connection ----- #
CON_STR = os.getenv("AZ_STORAGE_CONNECTION_STRING")
RAW_CONTAINER= os.getenv("DATA_CONTAINER_RAW", "raw")

svc = BlobServiceClient.from_connection_string(CON_STR)
container = svc.get_container_client(RAW_CONTAINER)


# ----- Upload to Azure Blob ----- #
def upload_to_blob(local_path: str, blob_name: str):

    """Upload local file to Azure Blob Storage"""

    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
    print(f"Uploaded {blob_name} -> container '{RAW_CONTAINER}'")


# ----- Download the Data files from URL/API ----- #
def download_file(url: str, local_path: str):

    """Download any file from the URL and save locally."""

    print(f"Dowloading {url}")
    r = requests.get(url)
    r.raise_for_status()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(r.content)
    print(f"Saved to {local_path}")


# ----- Get the URLs for the transit delay data for each year ----- #
def get_ttc_resource_url(year: int) -> str:

    """Query the Toronto Open Data API to get the TTC Dataset URL for the given year."""

    api_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=ttc-bus-delay-data"
    r = requests.get(api_url)
    r.raise_for_status()
    data = r.json()

    # Search all resources for one that match our year
    for resource in data["result"]["resources"]:
        fmt = resource["format"].lower()
        if str(year) in resource["name"] and fmt in ["csv", "xlsx"]:
            print(f"Found dataset for {year} : {resource['url']}")
            return resource['url'], fmt
        
    raise ValueError(f"No TTC resource found or year {year}")

# ------ Extract the Transit Dataset ------ #
def fetch_transit_data(year: int):

    """Download TTC Bus Delay Data dynamically for the given year"""

    url, fmt = get_ttc_resource_url(year)
    raw_path = f"data/raw/ttc_bus_delay_{year}.{fmt}"
    csv_path = f"data/raw/ttc_bus_delay_{year}.csv"

    download_file(url, raw_path)

    if fmt == "xlsx":
        print(f"Converting XLSX -> CSV for {year}")
        df = pd.read_excel(raw_path)
        df.to_csv(csv_path, index = False)
        os.remove(raw_path)
    else:
        csv_path = raw_path

    upload_to_blob(csv_path, f"ttc_bus_delay_{year}.csv")
    print(f"Completed processing for {year}")


# ------ Extract the Weather Data ------ #
def fetch_weather_data(year: int):
    lat, lon = 43.7, -79.4
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        f"&hourly=temperature_2m,precipitation"
        f"&timezone=America%2FToronto&format=csv"
    )
    local_path = f"data/weather/weather_{year}.csv"
    download_file(url, local_path)
    upload_to_blob(local_path, f"weather_{year}.csv")


# ----- Entry Point ----- #
if __name__ == "__main__":
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Starting extraction......")
    for year in [2023, 2024]:
        fetch_transit_data(year)
        fetch_weather_data(year)
        print(f"{year} Data extracted sucessfully")
    print("Extraction Complete.")
