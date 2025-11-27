import os
import pandas as pd
import pickle
from google.cloud import storage
from dotenv import load_dotenv
from sklearn.preprocessing import LabelEncoder
from io import StringIO


load_dotenv()

# ----- GCP Connection ------ #
GCP_BUCKET = os.getenv("GCP_BUCKET_NAME")
storage_client = storage.Client()
bucket = storage_client.bucket(GCP_BUCKET)


# ----- Read the Processed Data ----- #
def read_proc_blob(blob_name:str):
    blob_str = bucket.blob(f"processed/{blob_name}").download_as_text()
    df = pd.read_csv(StringIO(blob_str))
    print(f"Loaded the processed data from {blob_name}, shape = {df.shape}")

    return df

# ----- Upload the Data  and Encoders After Feature Eng. ----- #
def upload_to_blob(blob_name:str, local_path:str):
    blob = bucket.blob(f"{blob_name}")
    blob.upload_from_filename(local_path)

    print(f"Uploaded {blob_name} to container {GCP_BUCKET}")

# ----- Saving the encoders for inference ------ #
def save_encoders(encoders:dict):
    os.makedirs("models", exist_ok=True)

    path = "models/encoders.pkl"
    with open(path, "wb") as f:
        pickle.dump(encoders, f)
    upload_to_blob(path, path)
    


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
    df['incident'] = df['incident'].fillna("None")
    df['location'] = df['location'].fillna("Unknown")
    df.dropna(subset=['min_delay'], inplace=True)

    # Cap Outliers
    df['min_delay'] = df['min_delay'].clip(0, 300)

    # Encoding the categorical Variables
    cat_cols = ["route", "incident", "dayofweek", "location", "direction", "temp_bin", "rain_intensity"]
    encoders= {}
    
    for col in cat_cols:
        le = LabelEncoder()
        df[col] = df[col].astype(str)
        df[col] = le.fit_transform(df[col])
        encoders[col] = le
    save_encoders(encoders)

    df["is_delayed"] = (df["min_delay"] > 5).astype(int)
    
    #Dropping Unnecessary
    df.drop(columns=["time_x", "time_y", "date", "vehicle"], errors='ignore', inplace=True)

    print(f"Feature Engineering Complete ")
    return df

if __name__ == "__main__":
    print("Starting Feature Engineering Pipeline ....")

    df = read_proc_blob("transit_transformed_data_2023_2024.csv")

    df_feat_eng = feature_eng(df)

    
    cwd = os.getcwd()
    if "transitx-project" in cwd:
        root_index = cwd.index("transitx-project") + len("transitx-project")
        ROOT_DIR = cwd[:root_index]
    else:
    
        ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    DATA_DIR = os.path.join(ROOT_DIR, "data", "model_input")

    os.makedirs(DATA_DIR, exist_ok=True)
    local_path = os.path.join(DATA_DIR, "transit_features.csv")

    df_feat_eng.to_csv(local_path, index=False)
    print(f"Saved {local_path} locally.")

    upload_to_blob("model_input/transit_features.csv", local_path)
    

    print("Feature Engineering Completed Successfully :) ")


