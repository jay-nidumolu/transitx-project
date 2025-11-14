import os
import pandas as pd
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
from xgboost import XGBClassifier
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
    model = XGBClassifier(eval_metric = "logloss", random_state=42)

    search = RandomizedSearchCV(
        model,
        n_iter=20,
        param_distributions=param_dist,
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

    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    con_matrix = confusion_matrix(y_test, y_pred)

    print(f"Accuracy: {accuracy:.3f} \n F1 Score: {f1:.3f} \n Confusion Matrix : \n {con_matrix}")

    return accuracy, f1

    

# ----- Train the Model ----- #
def train_classifier_model(target, features, df):

    X_train, X_test, y_train, y_test = train_test_split(features, df[target], test_size=0.2, random_state=42)

    with mlflow_starter("transitx-classfier"):
        print("\n Training XGBoost Classifier (Delayed vs On-Time)\n")

        best_model=tune_model(X_train, y_train)

        accuracy, f1 = predict_eval_metrics(best_model, X_test, y_test)

        mlflow.log_metric("Accuracy", accuracy)
        mlflow.log_metric("F1 Score", f1)


        return best_model


if __name__ == "__main__":
    df = load_data()

    target = "is_delayed"
    features = df.drop(columns=["min_delay", target])

    assert all(features.dtypes != "object"), "Non-numeric columns found — check feature_eng.py"

    best_model = train_classifier_model(target, features, df)
    os.makedirs("models", exist_ok=True)
    save_path = "models/xgb_classfier.pkl"

    with open(save_path, "wb") as f:
        pickle.dump(best_model, f)

    upload_to_blob(save_path, "xgb_classifier.pkl")

    print("Classfier training Complete !) ")