import streamlit as st
import requests
from datetime import datetime

st.title("🔮 Live Transit Delay Prediction")
st.caption("Real-time inference using your deployed FastAPI model on Azure Container Apps.")

# ----------------------------------
# API URL
# ----------------------------------
API_URL = st.secrets["API_URL"]

st.info(f"Using API: `{API_URL}`")

# ----------------------------------
# Input Form
# ----------------------------------
st.subheader("📝 Enter Trip Details")

with st.form("prediction_form"):

    col1, col2 = st.columns(2)
    with col1:
        date = st.date_input("Date", datetime.now())
        time = st.time_input("Time", value=datetime.now().time())

        route = st.text_input("Route Number", "32")
        direction = st.selectbox("Direction", ["N", "S", "E", "W"])

    with col2:
        location = st.text_input("Location / Stop", "KENNEDY STATION")

        incident = st.selectbox(
            "Incident Type",
            [
                "None", "Mechanical", "General Delay", "Cleaning - Unsanitary",
                "Collision - TTC", "Security", "Diversion", "Held By",
                "Road Blocked - NON-TTC Collision"
            ],
            index=0
        )

        min_gap = st.number_input("Gap Between Buses (min)", min_value=0, value=10)

    submitted = st.form_submit_button("Predict Delay")

# ----------------------------------
# API Request
# ----------------------------------
#st.write("Testing health...")
health_url = API_URL.replace("/predict", "/health")

try:
    health = requests.get(health_url, timeout=10)
    #st.success(f"Health response: {health.text}")
except Exception as e:
    st.error(f"Health check failed: {e}")

if submitted:
    dt_str = date.strftime("%Y-%m-%d")
    tm_str = time.strftime("%H:%M")

    payload = {
        "date": dt_str,
        "time": tm_str,
        "route": route,
        "direction": direction,
        "location": location,
        "incident": incident,
        "min_gap": min_gap,
    }


    st.write("📨 Sending request to API...")
    try:
        response = requests.post(f"{API_URL}", json=payload, timeout=20)
        response.raise_for_status()
        result = response.json()

        ##st.success("Prediction received successfully!")

        # ----------------------------------
        # Output Card
        # ----------------------------------
        st.subheader("📘 Prediction Summary")

        st.markdown(f"""
        ### 🚌 Route {result['route']} — {result['direction']}
        **Location:** {result['location']}  
        **Datetime:** {result['datetime']}  

        ---
        ### 🔥 Prediction  
        - **Predicted Delay:** `{result['predicted_delay_minutes']} min`  
        - **Is Delayed?:** `{result['is_delayed']}`  
        - **Weather:** `{result['Weather_condition']}`  
        - **Rain:** `{result['rain_condition']}`  

        ---
        ### 🧠 Model Insight  
        _"{result['summary']}"_
        """)

    except Exception as e:
        st.error(f"❌ API Error: {e}")
