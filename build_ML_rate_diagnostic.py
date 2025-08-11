import os
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.impute import SimpleImputer
from pathlib import Path

# =====================================
# Config — Auto-detect agg input file
# =====================================
data_dir = Path("/mnt/data")
agg_files = list(data_dir.glob("v2_Rev_Perf_Weekly_Model_Output_Final_agg*"))

if not agg_files:
    raise FileNotFoundError("No file found matching 'v2_Rev_Perf_Weekly_Model_Output_Final_agg*' in /mnt/data")

AGG_IN = agg_files[0]  # use first match
AGG_OUT = data_dir / f"{AGG_IN.stem}_ml.csv"

print(f"📂 Using input file: {AGG_IN}")
print(f"📂 Output will be saved to: {AGG_OUT}")

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

# =====================================
# Step 0: Load aggregated file
# =====================================
if AGG_IN.suffix.lower() == ".csv":
    df = pd.read_csv(AGG_IN)
elif AGG_IN.suffix.lower() in [".xlsx", ".xls"]:
    df = pd.read_excel(AGG_IN)
else:
    raise ValueError(f"Unsupported file format: {AGG_IN.suffix}")

# =====================================
# Step 1: Coerce numerics
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

# Basic filters to avoid degenerate rows
df_model = df.dropna(subset=["Actual_Rate_per_Visit"]).copy()

feature_num = [
    "Visit_Count","Avg_Charge_EM_Weight","Labs_per_Visit","Procedure_per_Visit","Radiology_Count",
    "Zero_Balance_Collection_Rate","Collection_Rate","Denial_Percent",
    "NRV_Gap_Dollar","NRV_Gap_Percent","Remaining_Charges_Percent",
    "Expected_Payment","benchmark_payment"
]
feature_cat = ["Payer","Group_EM","Group_EM2"]

for col in feature_num:
    if col in df_model.columns:
        df_model[col] = df_model[col].apply(to_float_safe)
    else:
        df_model[col] = np.nan

for col in feature_cat:
    if col not in df_model.columns:
        raise ValueError(f"Missing categorical column: {col}")

# =====================================
# Step 2: Prepare data
# =====================================
df_model["_sort_key"] = list(zip(df_model["Year"], df_model["Week"]))
df_model = df_model.sort_values(["Year","Week"]).reset_index(drop=True)
X = df_model[feature_num + feature_cat]
y = df_model["Actual_Rate_per_Visit"].astype(float)

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

# =====================================
# Step 3: Model training
# =====================================
model = ElasticNet(alpha=0.1, l1_ratio=0.2, random_state=42, max_iter=2000)
pipe = Pipeline(steps=[("prep", preprocessor), ("model", model)])

# Cross-validation (time series split)
tscv = TimeSeriesSplit(n_splits=5)
cv_mae = []
cv_r2 = []
for train_idx, test_idx in tscv.split(X):
    pipe.fit(X.iloc[train_idx], y.iloc[train_idx])
    pred = pipe.predict(X.iloc[test_idx])
    cv_mae.append(mean_absolute_error(y.iloc[test_idx], pred))
    cv_r2.append(r2_score(y.iloc[test_idx], pred))

# Train on all data
pipe.fit(X, y)
pred_all = pipe.predict(X)

df_model["ML_Expected_Rate_per_Visit"] = pred_all
df_model["ML_Rate_Gap"] = df_model["Actual_Rate_per_Visit"] - df_model["ML_Expected_Rate_per_Visit"]
df_model["ML_Dollar_Gap"] = df_model["ML_Rate_Gap"] * df_model["Visit_Count"]

# Materiality flag
MATERIALITY_PER_VISIT = float(os.getenv("ML_MATERIALITY_PER_VISIT", "10"))
df_model["ML_Material_Gap_Flag"] = (df_model["ML_Rate_Gap"].abs() >= MATERIALITY_PER_VISIT).astype(int)

# Merge predictions back to full table
df_out = df.merge(
    df_model[["Year","Week","Payer","Group_EM","Group_EM2",
              "ML_Expected_Rate_per_Visit","ML_Rate_Gap","ML_Dollar_Gap","ML_Material_Gap_Flag"]],
    on=["Year","Week","Payer","Group_EM","Group_EM2"],
    how="left"
)

# =====================================
# Step 4: Save
# =====================================
df_out.to_csv(AGG_OUT, index=False)
print("✅ ML diagnostics written to:", AGG_OUT)
print(f"CV MAE (mean): {np.mean(cv_mae):.2f} | CV R^2 (mean): {np.mean(cv_r2):.3f}")
