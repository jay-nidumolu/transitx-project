import streamlit as st

st.set_page_config(
    page_title="TransitX Dashboard",
    page_icon="🚌",
    layout="wide"
)

st.title("🚌 TransitX — Transit Delay Prediction Dashboard")

st.markdown("""
Welcome to the **TransitX Dashboard**, an end-to-end machine learning system that predicts 
TTC bus delays using:

- Historical TTC delay data  
- Real-time & forecast weather (Open-Meteo API)  
- Route-level features  
- XGBoost ML models  
- FastAPI deployment on Azure Container Apps  

Use the sidebar to navigate between:

### 📊 **Analytics Overview**  
Explore historical delay patterns from processed datasets.

### ⚡ **Live Inference**  
Generate predictions instantly using your deployed **FastAPI API**.

---
""")
