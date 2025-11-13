import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from sklearn.preprocessing import LabelEncoder


load_dotenv()

# ----- Azure Setup ----- #
CONN_STR = os.getenv("AZ_STORAGE_CONNECTION_STRING")
PROC_CONTAINER = os.getenv("DATA_CONTAINER_PROCESSED", "processed")
MODEL_CONTAINER = os.getenv("DATA_CONTAINER_MODEL_INPUT", "model-input")

svc = BlobServiceClient.from_connection_string(CONN_STR)
proc_cont = svc.get_container_client(PROC_CONTAINER)
model_cont = svc.get_container_client(MODEL_CONTAINER)

# ----- Read the Processed Data ----- #
def read_proc_blob(blob_name:str):
    blob = proc_cont.download_blob(blob_name)

    df = pd.read_csv(blob)
    print(f"Loaded the processed data from {blob_name}, shape = {df.shape}")

    return df

# ----- Upload the Data After Feature Eng. ----- #
def upload_to_model_blob(blob_name:str, local_path:str):
    with open(local_path, "rb") as f:
        model_cont.upload_blob(name=blob_name, data=f, overwrite=True)
    print(f"Uploaded {blob_name} to container {MODEL_CONTAINER}")


# ------ Feature Engineering ------ #
def feature_eng(df:pd.DataFrame):
    print("Starting Feature Engineering....")

    # Clean Column names
    df.rename(columns={
        "min gap":"min_gap",
        "temperature_2m (°c)":"temperature",
        "precipitation (mm)":"precipitation",
        "day":"dayofweek"
    }, inplace=True)

    # Parse Datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['time_x'] = pd.to_datetime(df['time_x'], errors='coerce', format="%H:%M")
    df["hour"] = pd.to_datetime(df['time_x'], errors='coerce').dt.hour
    df["month"] = pd.to_datetime(df['date'], errors='coerce').dt.month

    # Binary Features
    df['rush_hour'] = df['hour'].isin([7,8,9,16,17,18]).astype(int)
    df['is_weekend'] = df['dayofweek'].astype(str).str.lower().isin(["saturday", "sunday"]).astype(int)

    # Weather category bins
    df["temp_bin"] = pd.cut(df["temperature"],
        bins=[-30,0,10,20,35],
        labels=["Freezing","Cold","Mild","Warm"]
    )
    df["rain_intensity"] = pd.cut(df["precipitation"],
        bins=[-0.1,0.1,2,5,10],
        labels=["None","Light","Moderate","Heavy"]
    )

    # Handling Missing
    df['direction'] = df['direction'].fillna("Unknown")
    df.dropna(subset=['min_delay'], inplace=True)

    # Cap Outliers
    df['min_delay'] = df['min_delay'].clip(0, 300)

    # Encoding the categorical Variables
    cat_cols = ["route", "incident", "dayofweek", "location", "direction", "temp_bin", "rain_intensity"]
    le = LabelEncoder()
    for col in cat_cols:
        df[col] = df[col].astype(str)
        df[col] = le.fit_transform(df[col])
    
    #Dropping Unnecessary
    df.drop(columns=["timex", "timey", "vehicle"], errors='ignore', inplace=True)

    print(f"Feature Engineering Complete ")
    return df

if __name__ == "__main__":
    print("Starting Feature Engineering Pipeline ....")

    df = read_proc_blob("transit_transformed_data_2023_2024.csv")

    df_feat_eng = feature_eng(df)

    os.makedirs("data/model_input", exist_ok=True)
    local_path = "data/model_input/transit_features.csv"

    df_feat_eng.to_csv(local_path, index=False)
    print(f"Saved {local_path} locally.")

    upload_to_model_blob("transit_features.csv", local_path)

    print("Feature Engineering Completed Successfully :) ")


