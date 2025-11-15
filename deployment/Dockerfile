# ----- Base Image ----- 
FROM python:3.12-slim

# ----- Working Directory ---- 
WORKDIR /app

# ------ Copy Dependencies ----
COPY ../requirements.txt .

# ------ Install Dependencies ------
RUN pip install --no-cache-dir -r requirements.txt

# ------- Copy Entire Project ------
COPY .. .

# ------- Expose Port -------
EXPOSE 8000

# ------- Start the App --------
CMD ["uvicorn", "deployment.app:app", "--host", "0.0.0.0", "--port", "8000"]
