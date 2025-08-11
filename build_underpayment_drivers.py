import os
import pandas as pd
import numpy as np
import zipfile

# =========================
# Config
# =========================
AGG_CSV = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_agg.csv"
GRANULAR_CSV = "/mnt/data/v2_Rev_Perf_Weekly_Model_Output_Final_granular.csv"
OUT_DIR = "/mnt/data"
PAYER_CSV = os.path.join(OUT_DIR, "underpayment_driver_payer.csv")
KEY_CSV = os.path.join(OUT_DIR, "underpayment_driver_benchmark_key.csv")
TIME_CSV = os.path.join(OUT_DIR, "underpayment_driver_time.csv")
ZIP_PATH = os.path.join(OUT_DIR, "underpayment_drivers.zip")

# =========================
# Helpers
# =========================
def to_float_safe(s):
    """Robust numeric parse: handles %, commas, blanks."""
    if pd.isna(s):
        return np.nan
    if isinstance(s, (int, float, np.integer, np.floating)):
        return float(s)
    s = str(s).strip().replace(",", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan

def negative_only(x):
    """Return x if negative; else 0.0 (for underpayment/shortfall)."""
    return x if (pd.notna(x) and x < 0) else 0.0

# =========================
# Load
# =========================
if not os.path.isfile(AGG_CSV) or not os.path.isfile(GRANULAR_CSV):
    raise FileNotFoundError("Run the weekly pipeline first to generate the aggregated and granular CSVs.")

agg = pd.read_csv(AGG_CSV)
gran = pd.read_csv(GRANULAR_CSV)

# Coerce numeric columns we need in agg
for col in ["Revenue_Variance", "Expected_vs_Benchmark_Payment_Variance_$", "Visit_Count"]:
    if col in agg.columns:
        agg[col] = agg[col].apply(to_float_safe)

# =========================
# 1) Aggregated view: compute shortfalls and incremental shortfall
# =========================
# Shortfall vs 85% Expected_Payment (negative only)
agg["Shortfall_vs_Expected_$"] = agg["Revenue_Variance"].apply(negative_only)

# Shortfall vs Benchmark_Payment (negative only)
agg["Shortfall_vs_Benchmark_$"] = agg["Expected_vs_Benchmark_Payment_Variance_$"].apply(negative_only)

# Incremental shortfall vs Benchmark beyond Expected benchmark (still negative values)
agg["Incremental_Shortfall_vs_Benchmark_$"] = agg["Shortfall_vs_Benchmark_$"] - agg["Shortfall_vs_Expected_$"]

# -------------------------
# Output 1: by Payer
# -------------------------
payer_breakdown = (
    agg.groupby("Payer", dropna=False)
       .agg(
           Shortfall_vs_Expected_$=("Shortfall_vs_Expected_$", "sum"),
           Shortfall_vs_Benchmark_$=("Shortfall_vs_Benchmark_$", "sum"),
           Incremental_Shortfall_vs_Benchmark_$=("Incremental_Shortfall_vs_Benchmark_$", "sum"),
           Total_Visit_Count=("Visit_Count", "sum")
       )
       .reset_index()
       .sort_values("Incremental_Shortfall_vs_Benchmark_$")
)
# Absolute magnitudes for quick ranking dashboards
payer_breakdown["Shortfall_vs_Expected_Abs_$"] = payer_breakdown["Shortfall_vs_Expected_$"].abs()
payer_breakdown["Shortfall_vs_Benchmark_Abs_$"] = payer_breakdown["Shortfall_vs_Benchmark_$"].abs()
payer_breakdown["Incremental_Shortfall_vs_Benchmark_Abs_$"] = payer_breakdown["Incremental_Shortfall_vs_Benchmark_$"].abs()

# =========================
# 2) Granular view: rebuild variances to attribute by Benchmark_Key
# =========================
# Coerce numeric columns we need in granular
for col in ["Visit_Count", "Payment_Amount", "Expected_Payment"]:
    if col in gran.columns:
        gran[col] = gran[col].apply(to_float_safe)

# Recompute row-level deltas (defensive)
gran["Revenue_Variance_Recalc"] = gran["Payment_Amount"] - gran["Expected_Payment"]
if "Benchmark_Payment" in gran.columns:
    gran["Expected_vs_Benchmark_Payment_Variance_$"] = gran["Expected_Payment"] - gran["Benchmark_Payment"]
else:
    gran["Expected_vs_Benchmark_Payment_Variance_$"] = np.nan

gran["Shortfall_vs_Expected_$"] = gran["Revenue_Variance_Recalc"].apply(negative_only)
gran["Shortfall_vs_Benchmark_$"] = gran["Expected_vs_Benchmark_Payment_Variance_$"].apply(negative_only)
gran["Incremental_Shortfall_vs_Benchmark_$"] = gran["Shortfall_vs_Benchmark_$"] - gran["Shortfall_vs_Expected_$"]

# -------------------------
# Output 2: by Benchmark_Key (with payer + E/M context)
# -------------------------
key_group_cols = ["Payer", "Group_EM", "Group_EM2", "Benchmark_Key"]
key_breakdown = (
    gran.groupby(key_group_cols, dropna=False)
        .agg(
            Shortfall_vs_Expected_$=("Shortfall_vs_Expected_$", "sum"),
            Shortfall_vs_Benchmark_$=("Shortfall_vs_Benchmark_$", "sum"),
            Incremental_Shortfall_vs_Benchmark_$=("Incremental_Shortfall_vs_Benchmark_$", "sum"),
            Total_Visit_Count=("Visit_Count", "sum")
        )
        .reset_index()
        .sort_values("Incremental_Shortfall_vs_Benchmark_$")
)
key_breakdown["Shortfall_vs_Expected_Abs_$"] = key_breakdown["Shortfall_vs_Expected_$"].abs()
key_breakdown["Shortfall_vs_Benchmark_Abs_$"] = key_breakdown["Shortfall_vs_Benchmark_$"].abs()
key_breakdown["Incremental_Shortfall_vs_Benchmark_Abs_$"] = key_breakdown["Incremental_Shortfall_vs_Benchmark_$"].abs()

# =========================
# 3) Time trend (when the gap opened)
# =========================
time_breakdown = (
    agg.groupby(["Year", "Week"], dropna=False)
       .agg(
           Shortfall_vs_Expected_$=("Shortfall_vs_Expected_$", "sum"),
           Shortfall_vs_Benchmark_$=("Shortfall_vs_Benchmark_$", "sum"),
           Incremental_Shortfall_vs_Benchmark_$=("Incremental_Shortfall_vs_Benchmark_$", "sum"),
           Total_Visit_Count=("Visit_Count", "sum")
       )
       .reset_index()
       .sort_values(["Year", "Week"])
)
time_breakdown["Shortfall_vs_Expected_Abs_$"] = time_breakdown["Shortfall_vs_Expected_$"].abs()
time_breakdown["Shortfall_vs_Benchmark_Abs_$"] = time_breakdown["Shortfall_vs_Benchmark_$"].abs()
time_breakdown["Incremental_Shortfall_vs_Benchmark_Abs_$"] = time_breakdown["Incremental_Shortfall_vs_Benchmark_$"].abs()

# =========================
# Save all three + zip
# =========================
payer_breakdown.to_csv(PAYER_CSV, index=False)
key_breakdown.to_csv(KEY_CSV, index=False)
time_breakdown.to_csv(TIME_CSV, index=False)

with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(PAYER_CSV, arcname=os.path.basename(PAYER_CSV))
    z.write(KEY_CSV, arcname=os.path.basename(KEY_CSV))
    z.write(TIME_CSV, arcname=os.path.basename(TIME_CSV))

print("✅ Built:")
print(" -", PAYER_CSV)
print(" -", KEY_CSV)
print(" -", TIME_CSV)
print("📦 Zip:", ZIP_PATH)
