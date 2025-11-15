from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
from datetime import datetime
import uvicorn
import pandas as pd
import pickle
import requests


# ------ Initializing the FASTAPI ------- #
app= FastAPI(
    title= "TransitX Delay Prediction API",
    description="Predicts if a TTC bus will be delayed and by how many minutes using live or historical conditions.",
    version="1.0"
)

print("API is running locally at: http://127.0.0.1:8000/docs")
print("When deployed on Azure, visit your container app URL + /docs")

@app.get("/")
def root():
    return {
        "message": "Welcome to TransitX API ",
        "local_docs_url": "http://127.0.0.1:8000/docs",
        "health_check": "/health",
        "note": "When deployed to Azure, use your cloud URL + /docs"
    }

@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.now().isoformat()
    }

VALID_INCIDENTS = {
    "Cleaning - Unsanitary",
    "Collision - TTC",
    "Diversion",
    "Emergency Services",
    "General Delay",
    "Held By",
    "Investigation",
    "Mechanical",
    "Operations - Operator",
    "Road Blocked - NON-TTC Collision",
    "Security",
    "Utilized Off Route",
    "Vision",
    "None"  # for predictions where no incident is expected
}

# -- Input Schema -- #
class TransitInput(BaseModel):
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    time: str = Field(..., description="Time in HH:MM format")
    route: str = Field(..., example="32", description="Bus route number (numeric only)")
    direction: str = Field(..., example="E", description="Direction (N/S/E/W or full name)")
    location: str = Field(..., example="KENNEDY STATION", description="Known TTC stop or station")
    incident: str = Field(default="None", example="Mechanical", description="Incident type or 'None'")
    min_gap: int = Field(default=10, ge=0, description="Gap between buses (in minutes)")

    @validator("date")
    def validate_date(cls, value):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="❌ Invalid date format. Use YYYY-MM-DD (e.g., 2024-05-12)."
            )
        return value

    @validator("time")
    def validate_time(cls, value):
        try:
            datetime.strptime(value, "%H:%M")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="❌ Invalid time format. Use HH:MM (e.g., 09:15)."
            )
        return value

    @validator("route")
    def validate_route(cls, value):
        if not value.isdigit():
            raise HTTPException(
                status_code=400,
                detail="❌ Route must be numeric (e.g., '32', '91', '505')."
            )
        return value

    @validator("direction", pre=True)
    def normalize_direction(cls, value):
        value = value.strip().capitalize()
        if value in {"N", "S", "E", "W"}:
            return value
        if value.lower() in {"north", "northbound"}:
            return "N"
        if value.lower() in {"south", "southbound"}:
            return "S"
        if value.lower() in {"east", "eastbound"}:
            return "E"
        if value.lower() in {"west", "westbound"}:
            return "W"
        raise HTTPException(status_code=400, detail=f"Invalid direction '{value}'. Use N/S/E/W.")

    @validator("incident")
    def validate_incident(cls, value):
        if value not in VALID_INCIDENTS:
            raise HTTPException(
                status_code=400,
                detail=f"❌ Invalid incident '{value}'. Choose from: {', '.join(VALID_INCIDENTS)}."
            )
        return value


# ----- Load the models ----- #
def load_pkl_file(file_path):
    with open(file_path, "rb") as f:
        model = pickle.load(f)
    return model

# ---- Extract date and time features ---- #
def time_features(date_str:str, time_str:str):
    dt = datetime.fromisoformat(f"{date_str}T{time_str}")
    hour = dt.hour
    month = dt.month
    dayofweek = dt.strftime("%A")
    rush_hour = 1 if hour in [7,8,9,16,17,18] else 0
    is_weekend = 1 if dayofweek.lower() in ["saturday", "sunday"] else 0

    return dt, hour, month, dayofweek, rush_hour, is_weekend

# ---- fetch weather data ----- #
def fetch_weathe_data(dt:datetime):
    now = datetime.now()
    is_past = dt.date() < now.date()

    if is_past:
        base_url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 43.7,
            "longitude": -79.4,
            "start_date": dt.date(),
            "end_date": dt.date(),
            "hourly": "temperature_2m,precipitation",
            "timezone": "America/Toronto",
            "format": "json"
        }
    else:
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
             "latitude": 43.7,
            "longitude": -79.4,
            "hourly": "temperature_2m,precipitation",
            "timezone": "America/Toronto",
            "forecast_days": 7,
    }
    
    try:
        res = requests.get(base_url, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        hour_idx = dt.hour

        temp = data["hourly"]["temperature_2m"][hour_idx]
        rain = data["hourly"]["precipitation"][hour_idx]

        return temp, rain
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not fetch weather data for {dt.date()} as open-meteo api can predict till 16 days from today: {str(e)}"
        )
        return 10.0, 0.0

# ----- Weather Categories ----- #
def categorize_weather(temp, rain):
    if temp <= 0:
        temp_bin = "Freezing"
    elif temp <= 10:
        temp_bin = "Cold"
    elif temp <= 20:
        temp_bin = "Mild"
    else:
        temp_bin = "Warm"

    if rain <= 0.1:
        rain_bin = "None"
    elif rain <= 2:
        rain_bin = "Light"
    elif rain <= 5:
        rain_bin = "Moderate"
    else:
        rain_bin = "Heavy"

    return temp_bin, rain_bin

# ---- Encode categorical input ----- #
def encode_cat_input(df: pd.DataFrame, encoders: dict):
    df_copy = df.copy()
    for col, le in encoders.items():
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str)
            
            known_classes = set(le.classes_)
            df_copy[col] = df_copy[col].apply(lambda x: x if x in known_classes else "Unknown")
            
            # If "Unknown" wasn’t in the original encoder, add it dynamically
            if "Unknown" not in known_classes:
                import numpy as np
                le.classes_ = np.append(le.classes_, "Unknown")
            
            df_copy[col] = le.transform(df_copy[col])
    return df_copy

#------ Prepare the data for predictions ------- #
def prepare_data(input_data:TransitInput):
    try:
        parsed_date = datetime.fromisoformat(input_data.date)
        date_str = parsed_date.strftime("%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=" Invalid date or time format. Use YYYY-MM-DD for date and HH:MM for time (e.g., 2024-05-12, 09:15)."
        )

    time_str = input_data.time

    dt, hour, month, dayofweek, rush_hour, is_weekend = time_features(date_str, time_str)
    temp, precipitation = fetch_weathe_data(dt)
    temp_bin, rain_intensity = categorize_weather(temp, precipitation)

    df = pd.DataFrame([{
        "route": input_data.route,
        "dayofweek":dayofweek,
        "location":input_data.location,
        "incident":input_data.incident,
        "min_gap": input_data.min_gap,
        "direction":input_data.direction,
        "temperature":temp,
        "precipitation":precipitation,
        "hour":hour,
        "month":month,
        "rush_hour": rush_hour,
        "is_weekend":is_weekend,
        "temp_bin":temp_bin,
        "rain_intensity":rain_intensity,
        
    }])

    encoder_file_path = "models/encoders.pkl"
    encoder = load_pkl_file(encoder_file_path)
    
    encoded_df = encode_cat_input(df, encoder)

    return encoded_df, date_str


# ----- Summary helper -----#
def generate_summary(temp_bin_name, rain_intensity_name, delay_minutes, is_delayed):
    # Describe weather
    if rain_intensity_name in ["Heavy", "Moderate"]:
        weather_comment = "Wet conditions may slow down traffic."
    elif temp_bin_name in ["Freezing", "Cold"]:
        weather_comment = "Cold temperatures might slightly impact operations."
    else:
        weather_comment = "Weather conditions are normal."

    # Describe delay outcome
    if is_delayed:
        delay_comment = f"Expected delay of around {delay_minutes} minutes."
    else:
        delay_comment = "Bus is expected to be on time."

    return f"{weather_comment} {delay_comment}"


@app.post("/predict")
def predict(input_data:TransitInput):

    input_df, date_str = prepare_data(input_data)

    reg_model_path = "models/xgb_regressor.pkl"

    reg_model = load_pkl_file(reg_model_path)

    # Make predictions
    delay_minutes = round(float(reg_model.predict(input_df)[0]))
    is_delayed = delay_minutes > 3

    encoder = load_pkl_file("models/encoders.pkl")
    temp_bin_encoder = encoder.get("temp_bin")
    rain_encoder = encoder.get("rain_intensity")


    temp_bin_name = (
        temp_bin_encoder.inverse_transform([int(input_df["temp_bin"].iloc[0])])[0]
        if temp_bin_encoder else "Unknown"
    )
    rain_intensity_name = (
        rain_encoder.inverse_transform([int(input_df["rain_intensity"].iloc[0])])[0]
        if rain_encoder else "Unknown"
    )

    summary_text = generate_summary(
        temp_bin_name, rain_intensity_name, delay_minutes, bool(is_delayed)
    )

    response = {
        "datetime": f"{date_str} {input_data.time}",
        "route": input_data.route,
        "direction": input_data.direction,
        "location": input_data.location,
        "incident": input_data.incident,
        "predicted_delay_minutes": delay_minutes,
        "is_delayed": bool(is_delayed),
        "temperature_C": float(input_df["temperature"].iloc[0]),
        "precipitation_mm": float(input_df["precipitation"].iloc[0]),
        "Weather_condition": temp_bin_name,
        "rain_condition": rain_intensity_name,
        "summary":summary_text
    }

    return response

if __name__ =="__main__":

    uvicorn.run("deployment.app:app", host="127.0.0.1", port=8000, reload=True)
