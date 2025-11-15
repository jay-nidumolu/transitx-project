# ---------- Setup ----------
setup:
	python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# ---------- ETL ----------
run-pipeline:
	python3 main.py

# ---------- Model Training ----------
train-reg:
	python3 src/models/train_regressor.py

train-cls:
	python3 src/models/train_classifier.py

# ---------- Docker (Local + Prod) ----------
build-local:
	docker build -t jaynid00/transitx-api:dev -f deployment/Dockerfile .

run-local:
	docker run -p 8000:8000 jaynid00/transitx-api:dev

build-prod:
	docker buildx build --platform linux/amd64 -t jaynid00/transitx-api:prod -f deployment/Dockerfile .

push-prod:
	docker push jaynid00/transitx-api:prod

# ---------- DVC ----------
dvc-init:
	dvc init
	dvc remote add -d azure-storage azure://<your-container-name>

dvc-track:
	dvc add data/model_input/transit_features.csv models/xgb_regressor.pkl
	git add .dvc .gitignore
	git commit -m "Track data and model with DVC"

# ---------- Azure Deployment ----------
deploy-azure:
	az containerapp create \
	--name transitx-api-app \
	--resource-group transitx-rg \
	--environment transitx-api \
	--image jaynid00/transitx-api:prod \
	--target-port 8000 \
	--ingress external
