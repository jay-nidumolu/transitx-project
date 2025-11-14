import os
import pandas as pd
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor
import pickle
import mlflow
import mlflow.sklearn
from src.utils.model_utils import load_data, upload_to_blob, mlflow_starter

# ---- Hyperparameter Tuning ----- #
def tune_model(X_train, y_train):
    param_dist = {
        "n_estimators": [200, 300, 500],
        "max_depth": [4,6,8,10],
        "learning_rate": [0.01,0.05, 0.1],
        "subsample":[0.7, 0.9, 1.0],
        "colsample_bytree":[0.7,1.0],
        "gamma":[0,2,5]
    }
    model = XGBRegressor(random_state=42)

    search = RandomizedSearchCV(
        model,
        n_iter=20,
        param_distributions=param_dist,
        scoring="neg_mean_absolute_error",
        cv=3,
        verbose=2,
        n_jobs=1, 
        random_state=42)
    
    search.fit(X_train, y_train)
    print("Best parameters: ", search.best_params_)

    return search.best_estimator_

# --- Prediction and Evaluation of metrics --- #
def predict_eval_metrics(model, X_test, y_test):
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"Mean_absolute Error: {mae:.3f} \n Mean Squared Error: {mse:.3f} \n R2:{r2:.2f}")

    return mae, mse, r2

    

# ----- Train the Model ----- #
def train_reg_model(target, features, df):

    X_train, X_test, y_train, y_test = train_test_split(features, df[target], test_size=0.2, random_state=42)

    with mlflow_starter("transitx-regressor"):
        print("\n Training XGBoost Regressor (Predicting Delay Minutes)\n")

        best_model=tune_model(X_train, y_train)

        mae, mse, r2 = predict_eval_metrics(best_model, X_test, y_test)

        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        return best_model


if __name__ == "__main__":
    df = load_data()

    target = "min_delay"
    features = df.drop(columns=[target, "is_delayed"])

    assert all(features.dtypes != "object"), "Non-numeric columns found — check feature_eng.py"

    best_model = train_reg_model(target, features, df)

    os.makedirs("models", exist_ok=True)
    save_path = "models/xgb_regressor.pkl"
    with open (save_path, "wb") as f:
        pickle.dump(best_model, f)

    upload_to_blob(save_path, "xgb_regressor.pkl")

    print("Regressor training Complete !) ")