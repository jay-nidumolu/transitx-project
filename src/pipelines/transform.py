import os
import pandas as pd
from google.cloud import storage
from io import StringIO
from dotenv import load_dotenv

load_dotenv()


# -------- GCP Connections -------- #
GCP_BUCKET = os.getenv("GCP_BUCKET_NAME")

storage_client = storage.Client()
bucket = storage_client.bucket(GCP_BUCKET)


# ----- Downloading the File from the Blob ----- #
def read_blob_csv(name):

    blob = bucket.blob(f"raw/{name}")
    content = blob.download_as_text()
    lines = content.splitlines()

    if any("latitude" in line.lower() for line in lines[:10]) and any(
        line.lower().startswith("time,") for line in lines
    ):
        header_line_idx = next(
            (i for i, line in enumerate(lines) if line.lower().startswith("time,")), 0
        )
        clean_csv = "\n".join(lines[header_line_idx:])
        df = pd.read_csv(StringIO(clean_csv))
        print(f" Cleaned weather file detected: {name} (skipped {header_line_idx} lines)")
    else:
        df = pd.read_csv(StringIO(content))
        print(f"Clean TTC/other file detected: {name}")
    
    return df


# ----- Upload the Data frame to Blob ------ #
def upload_df_blob(df, name):
    os.makedirs("data/processed", exist_ok=True)
    local_path = "data/processed/transit_processed.csv"
    df.to_csv(local_path, index=False)
    blob = bucket.blob(f"processed/{name}")
    blob.upload_from_filename(local_path)
    print("Uploaded the processed file to the blob")

# ----- Transforming the dataframes ------ #
def transformer(dfs, kind="delay"):
    dataframe = pd.concat(dfs)
    dataframe.columns = [c.lower().strip() for c in dataframe.columns]

    if kind =="delay":
        dataframe["date"] = pd.to_datetime(dataframe["date"], errors= "coerce")

        possible_delay_cols = ["min_delay", "min delay", "min delay (min)", "min delay (mins)", "min delay mins"]
        delay_col = next((c for c in possible_delay_cols if c in dataframe.columns), None)
        if delay_col:
            dataframe.rename(columns = {delay_col: "min_delay"}, inplace=True)
            dataframe["min_delay"] = pd.to_numeric(dataframe["min_delay"], errors = "coerce")
        else:
            raise KeyError(" Could not find 'min_delay' column in TTC dataset.")

        dataframe = dataframe.dropna(subset=["min_delay", "route"])
    elif kind == "weather":
        

        time_col = "time" if "time" in dataframe.columns else "timestamp"
        dataframe[time_col] = pd.to_datetime(dataframe[time_col], errors = "coerce")
    else:
        print("Unknown kind: {kind} -  no specific rules applied")

    return dataframe
    

if __name__ == "__main__":
    print("Starting Transformatons...")

    delay_dfs = [read_blob_csv(f"ttc_bus_delay_{yr}.csv") for yr in ["2023", "2024"]]
    delay = transformer(delay_dfs, kind = "delay")
    
    weather_dfs = [read_blob_csv(f"weather_{yr}.csv") for yr in ["2023", "2024"]]
    weather = transformer(weather_dfs, kind = "weather")

    right_key = "time" if "time" in weather.columns else "timestamp"

    merged_df = pd.merge(delay, weather, left_on="date", right_on=right_key, how="left")

    print("Merged shape: {merged_df.shape}")
    upload_df_blob(merged_df, "transit_transformed_data_2023_2024.csv")
    print("Transformation Complete ;)")
