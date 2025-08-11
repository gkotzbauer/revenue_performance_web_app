#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import math
import time
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingRegressor

# =====================================
# Config — auto-detect agg input file
# =====================================
DATA_DIR = Path("/mnt/data")
CANDIDATES = sorted(
    list(DATA_DIR.glob("v2_Rev_Perf_Weekly_Model_Output_Final_agg*")),
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)

if not CANDIDATES:
    raise FileNotFoundError("No file found matching 'v2_Rev_Perf_Weekly_Model_Output_Final_agg*' in /mnt/data")

AGG_IN = CANDIDATES[0]
AGG_OUT = DATA_DIR / f"{AGG_IN.stem}_ml_boosted.csv"
METRICS_JSON = DATA_DIR / "ml_model_performance.json"             # rolling (latest)
TIMESTAMPED_JSON = DATA_DIR / f"ml_model_performance_{int(time.time())}.json"  # history

print(f"📂 Using input file: {AGG_IN}")
print(f"📂 Output will be saved to: {AGG_OUT}")
print(f"🧪 Metrics JSON will be saved to: {METRICS_JSON} (and timestamped copy)")

# =====================================
# Env knobs (with sensible defaults)
# =====================================
TS_SPLITS   = int(os.getenv("ML_TS_SPLITS", "5"))
ROW_CAP     = int(os.getenv("ML_ROW_CAP", "25000"))       # cap for faster CV
LR          = float(os.getenv("ML_HGB_LR", "0.06"))
ITERS       = int(os.getenv("ML_HGB_ITERS", "400"))
MAX_DEPTH   = os.getenv("ML_HGB_MAX_DEPTH", "None")
MAX_DEPTH   = None if MAX_DEPTH == "None" else int(MAX_DEPTH)
MATERIALITY = float(os.getenv("ML_MATERIALITY_PER_VISIT", "10"))

# =====================================
# Helpers
# =====================================
def to_float_safe(x):
    if pd.isna(x): return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)): return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"):
        try: return float(s[:-1]) / 100.0
        except: return np.nan
    try: return float(s)
    except: return np.nan

def load_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file format: {path.suffix}")

# =====================================
# Step 0: Load aggregated file
# =====================================
df = load_any(AGG_IN)

# =====================================
# Step 1: Numeric coercion
# =====================================
num_cols_to_parse = [
    "Payment_Amount","Visit_Count","Avg_Charge_EM_Weight","Labs_per_Visit",
    "Procedure_per_Visit","Radiology_Count",
    "Zero_Balance_Collection_Rate","Collection_Rate","Denial_Percent",
    "NRV_Gap_Dollar","NRV_Gap_Percent","Remaining_Charges_Percent",
    "Expected_Payment","benchmark_payment"
]
for c in num_cols_to_parse:
    if c in df.columns:
        df[c] = df[c].apply(to_float_safe)

# Target: Actual rate per visit
df["Actual_Rate_per_Visit"] = np.where(df["Visit_Count"] == 0, np.nan, df["Payment_Amount"]/df["Visit_Count"])

# Basic modeling frame
df_model = df.dropna(subset=["Actual_Rate_per_Visit"]).copy()

feature_num = [
    "Visit_Count","Avg_Charge_EM_Weight","Labs_per_Visit","Procedure_per_Visit","Radiology_Count",
    "Zero_Balance_Collection_Rate","Collection_Rate","Denial_Percent",
    "NRV_Gap_Dollar","NRV_Gap_Percent","Remaining_Charges_Percent",
    "Expected_Payment","benchmark_payment"
]
feature_cat = ["Payer","Group_EM","Group_EM2"]

# Add missing numeric columns as NaN (keeps schema stable)
for col in feature_num:
    if col not in df_model.columns:
        df_model[col] = np.nan
# Validate categoricals
for col in feature_cat:
    if col not in df_model.columns:
        raise ValueError(f"Missing categorical column: {col}")

# Sort time for TimeSeriesSplit
if not {"Year","Week"}.issubset(df_model.columns):
    raise ValueError("Missing Year/Week columns required for time-based CV.")
df_model = df_model.sort_values(["Year","Week"]).reset_index(drop=True)

# Optional row cap (for very large sets)
if ROW_CAP and len(df_model) > ROW_CAP:
    df_model = df_model.iloc[-ROW_CAP:].reset_index(drop=True)  # keep latest window

X = df_model[feature_num + feature_cat]
y = df_model["Actual_Rate_per_Visit"].astype(float)

# =====================================
# Step 2: Pipeline
# =====================================
numeric_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median"))
])
categorical_transformer = Pipeline(steps=[
    ("ohe", OneHotEncoder(handle_unknown="ignore"))
])

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, feature_num),
        ("cat", categorical_transformer, feature_cat),
    ],
    remainder="drop"
)

model = HistGradientBoostingRegressor(
    learning_rate=LR,
    max_depth=MAX_DEPTH,
    max_iter=ITERS,
    random_state=42,
)

pipe = Pipeline(steps=[("prep", preprocessor), ("model", model)])

# =====================================
# Step 3: TimeSeries CV (diagnostics)
# =====================================
tscv = TimeSeriesSplit(n_splits=TS_SPLITS)
cv_metrics = {
    "fold": [],
    "MAE": [],
    "RMSE": [],
    "R2": [],
}

for fold, (train_idx, test_idx) in enumerate(tscv.split(X), start=1):
    pipe.fit(X.iloc[train_idx], y.iloc[train_idx])
    pred = pipe.predict(X.iloc[test_idx])
    yt = y.iloc[test_idx].values

    mae = mean_absolute_error(yt, pred)
    rmse = math.sqrt(mean_squared_error(yt, pred))
    r2 = r2_score(yt, pred)

    cv_metrics["fold"].append(fold)
    cv_metrics["MAE"].append(mae)
    cv_metrics["RMSE"].append(rmse)
    cv_metrics["R2"].append(r2)

# =====================================
# Step 4: Train on all data & predict
# =====================================
pipe.fit(X, y)
pred_all = pipe.predict(X)

df_model["HGB_Expected_Rate_per_Visit"] = pred_all
df_model["HGB_Rate_Gap"] = df_model["Actual_Rate_per_Visit"] - df_model["HGB_Expected_Rate_per_Visit"]
df_model["HGB_Dollar_Gap"] = df_model["HGB_Rate_Gap"] * df_model["Visit_Count"]
df_model["HGB_Material_Gap_Flag"] = (df_model["HGB_Rate_Gap"].abs() >= MATERIALITY).astype(int)

# Train-set summary metrics (for reference)
train_mae  = mean_absolute_error(y, pred_all)
train_rmse = math.sqrt(mean_squared_error(y, pred_all))
train_r2   = r2_score(y, pred_all)

# =====================================
# Step 5: Merge predictions back to full table
# =====================================
df_out = df.merge(
    df_model[["Year","Week","Payer","Group_EM","Group_EM2",
              "HGB_Expected_Rate_per_Visit","HGB_Rate_Gap","HGB_Dollar_Gap","HGB_Material_Gap_Flag"]],
    on=["Year","Week","Payer","Group_EM","Group_EM2"],
    how="left"
)

# =====================================
# Step 6: Persist outputs
# =====================================
df_out.to_csv(AGG_OUT, index=False)

# Metrics JSON (rolling + timestamped)
metrics_payload = {
    "input_file": str(AGG_IN),
    "output_file": str(AGG_OUT),
    "rows_used_for_model": int(len(df_model)),
    "env": {
        "ML_TS_SPLITS": TS_SPLITS,
        "ML_ROW_CAP": ROW_CAP,
        "ML_HGB_LR": LR,
        "ML_HGB_ITERS": ITERS,
        "ML_HGB_MAX_DEPTH": None if MAX_DEPTH is None else int(MAX_DEPTH),
        "ML_MATERIALITY_PER_VISIT": MATERIALITY,
    },
    "cross_validation": {
        "folds": cv_metrics["fold"],
        "MAE_per_fold": cv_metrics["MAE"],
        "RMSE_per_fold": cv_metrics["RMSE"],
        "R2_per_fold": cv_metrics["R2"],
        "MAE_mean": float(np.mean(cv_metrics["MAE"])),
        "RMSE_mean": float(np.mean(cv_metrics["RMSE"])),
        "R2_mean": float(np.mean(cv_metrics["R2"])),
        "MAE_std": float(np.std(cv_metrics["MAE"], ddof=1)) if len(cv_metrics["MAE"]) > 1 else 0.0,
        "R2_std": float(np.std(cv_metrics["R2"], ddof=1)) if len(cv_metrics["R2"]) > 1 else 0.0,
    },
    "train_fit": {
        "MAE": float(train_mae),
        "RMSE": float(train_rmse),
        "R2": float(train_r2),
    },
}

with open(METRICS_JSON, "w") as f:
    json.dump(metrics_payload, f, indent=2)
with open(TIMESTAMPED_JSON, "w") as f:
    json.dump(metrics_payload, f, indent=2)

print("✅ Boosted ML diagnostics written to:", AGG_OUT)
print(f"📊 Metrics JSON (latest): {METRICS_JSON}")
print(f"🕒 Metrics JSON (timestamped): {TIMESTAMPED_JSON}")
print(
    f"CV MAE (mean): {metrics_payload['cross_validation']['MAE_mean']:.2f} | "
    f"CV R² (mean): {metrics_payload['cross_validation']['R2_mean']:.3f} | "
    f"Train R²: {metrics_payload['train_fit']['R2']:.3f}"
)
