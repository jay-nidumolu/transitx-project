import os
import pickle
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient

load_dotenv()


# ----- Load the Model ----- #
def load_model(model_path:str):
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    print(f"Loaded model from {model_path}")
    return model

# ----- Generate Predictions ----- #
def generate_predictions(df:pd.DataFrame, model, model_name:str):
    print(f"Generating Predictions for {model_name}......")
    preds = model.predict(df)

    return preds


# ----- Upload to Blob ----- #
def upload_to_blob(path:str, blob_name:str):
    conn_str = os.getenv("AZ_STORAGE_CONNECTION_STRING")
    svc = BlobServiceClient.from_connection_string(conn_str)
    container = svc.get_container_client(os.getenv("PREDICTIONS", "predictions"))

    with open(path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
        print(f"Uploaded predictions -> Azure Blob: {container}/{blob_name}")

if __name__=="__main__":
    print("Starting Batch Predictions......")

    input_file = "data/model_input/transit_features.csv"

    df_original = pd.read_csv(input_file)

    reg_model_path = "models/xgb_regressor.pkl"
    class_model_path = "models/xgb_classifier.pkl"

    reg_model = load_model(reg_model_path)
    class_model = load_model(class_model_path)

    df_model_input = df_original.copy()
    for col in ["min_delay", "is_delayed"]:
        if col in df_model_input.columns:
            df_model_input.drop(columns=[col], inplace=True)

    pred_delay_minutes = generate_predictions(df_model_input, reg_model, "Regression Model").round()
    print("Predictions Generated Successfully for Regression Model.")
    pred_is_delayed = generate_predictions(df_model_input, class_model, "Classifier Model").round()
    print("Predictions Generated Successfully for Classifier Model.")

    df_original["pred_delay_minutes"] = pred_delay_minutes
    df_original["pred_is_delayed"]= pred_is_delayed

    os.makedirs("data/predictions", exist_ok=True)
    output_path = "data/predictions/transit_predictions.csv"
    df_original.to_csv(output_path, index=False)

    upload_to_blob(output_path, "transit_predictions.csv")

    print("Batch predictions completed Successfully.")

    




