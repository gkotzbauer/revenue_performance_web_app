import os
import re
import pandas as pd
import numpy as np
import zipfile

# === Step 0: File Paths ===
SOURCE_FILE = "/mnt/data/v2 Rev Perf Report with Second Group Layer(4).xlsx"
CSV_FILENAME = "Invoice_Assigned_To_Benchmark_With_Count.csv"
CSV_PATH = f"/mnt/data/{CSV_FILENAME}"
ZIP_PATH = "/mnt/data/Invoice_Assigned_To_Benchmark_With_Count.zip"
VALIDATION_REPORT = "/mnt/data/validation_report.csv"

# === Step 1: Metric Rules (kept for consistency) ===
increase_good = {
    "Visit Count": True,
    "Avg. Charge E/M Weight": True,
    "Charge Amount": True,
    "Charge Billed Balance": False,
    "Zero Balance - Collection * Charges": False,
    "Payment per Visit": True,
    "NRV Zero Balance*": True,
    "Zero Balance Collection Rate": True,
    "Collection Rate*": True,
    "Labs per Visit": True,
    "Payment Amount*": True,
    "Avg. Payment per Visit By Payor": True,
    "Avg. Payments By Payor": True,
    "NRV Gap ($)": False,
    "NRV Gap (%)": False,
    "NRV Gap Sum ($)": True,
    "% of Remaining Charges": False,
    "Radiology Count": True,
    "Denial %": False,
    "Insurance Charge Billed Balance": False,
    "SP Charge Billed Balance": False,
    "AR Over 90": False,
    "Procedure per Visit": True,
    "Expected Amount (85% E/M)": True,
    "Fee Schedule Expected Amount": True,
    "Charge Per Visit": True,
    "Open Invoice Count": False
}

# === Step 2: Load & Clean Source Data ===
if not os.path.isfile(SOURCE_FILE):
    raise FileNotFoundError(f"Error: File not found: {SOURCE_FILE}")

df = pd.read_excel(SOURCE_FILE, sheet_name=0)
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# 2A) Normalize all cells: strip whitespace, convert blanks to NaN
df = df.applymap(lambda x: np.nan if pd.isna(x) or str(x).strip() == "" else x)

# 2B) Standardize column names
df = df.rename(columns={
    "Year of Visit Service Date": "Year",
    "ISO Week of Visit Service Date": "Week",
    "Primary Financial Class": "Payer",
    "Chart E/M Code Grouping": "Group_EM",
    "Chart E/M Code Second Layer": "Group_EM2",
    "Charge Invoice Number": "Invoice_Number"
})

# 2C) Fill missing metadata fields via forward-fill (preserves block headers)
df[["Year", "Week", "Payer", "Group_EM", "Group_EM2"]] = (
    df[["Year", "Week", "Payer", "Group_EM", "Group_EM2"]].ffill().astype(str)
)
df["Year"] = df["Year"].str.replace(".0", "", regex=False)
df["Week"] = df["Week"].str.extract(r"(\d+)", expand=False).astype(float).fillna(0).astype(int)
df["Invoice_Number"] = df["Invoice_Number"].ffill()

# === Step 2D: Identify numeric columns by format type ===
currency_cols = [c for c in df.columns if "$" in c or "Amount" in c or "Balance" in c]
percent_cols = [c for c in df.columns if "%" in c or "Rate" in c]
count_cols = [c for c in df.columns if "Count" in c or "Visit" in c or "Procedure" in c or "Radiology" in c]

# Convert all to numeric and fill NaNs with 0.00 for currency/percent, 0 for counts
for col in currency_cols + percent_cols + count_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")
    if col in currency_cols or col in percent_cols:
        df[col] = df[col].fillna(0.00)
    else:
        df[col] = df[col].fillna(0)

# === Step 3: Required Field Validation ===
required_cols = ["Invoice_Number", "Payer", "Group_EM", "Group_EM2", "Charge CPT Code"]
missing_required = df[required_cols].isna().any(axis=1)
validation_report = df[missing_required].copy()
df = df[~missing_required].copy()
validation_report.to_csv(VALIDATION_REPORT, index=False)

# === Step 4: Zero-Payment Handling & Derived Metrics ===
zero_mask = (df["Payment Amount*"] == 0)
if "Charge Billed Balance" not in df.columns:
    alt_cols = [c for c in df.columns if "Charge Billed" in c and "Balance" in c]
    if alt_cols:
        df["Charge Billed Balance"] = pd.to_numeric(df[alt_cols[0]], errors="coerce").fillna(0)

df.loc[zero_mask, ["Payment per Visit", "NRV Zero Balance*", "Zero Balance Collection Rate", "Collection Rate*"]] = 0
df.loc[zero_mask, "Zero Balance - Collection * Charges"] = df.loc[zero_mask, "Charge Billed Balance"]

df["% of Remaining Charges"] = np.where(
    df["Charge Amount"] == 0,
    0,  # Now set to 0 instead of NaN
    df["Charge Billed Balance"] / df["Charge Amount"]
)

# Drop invalid Avg. Charge E/M Weight values
if "Avg. Charge E/M Weight" in df.columns:
    df.loc[~df["Group_EM"].isin({"Existing E/M Code", "New E/M Code"}), "Avg. Charge E/M Weight"] = 0

# === Step 5: Build CPT List + Benchmark Keys ===
df["Charge CPT Code"] = df["Charge CPT Code"].astype(str).str.strip()

cpt_list_df = (
    df.groupby("Invoice_Number", dropna=False)["Charge CPT Code"]
      .apply(lambda x: sorted(set(x)))
      .reset_index()
      .rename(columns={"Charge CPT Code": "CPT_List"})
)
cpt_list_df["CPT_List_Str"] = cpt_list_df["CPT_List"].apply(str)

df = df.merge(cpt_list_df, on="Invoice_Number", how="left")

df["Abbreviate_Benchmark_Key"] = (
    df["Invoice_Number"].astype(str) + "|" +
    df["Payer"].astype(str) + "|" +
    df["Group_EM"].astype(str) + "|" +
    df["Group_EM2"].astype(str) + "|" +
    df["CPT_List_Str"].astype(str)
)

df["Benchmark_Key"] = (
    df["Payer"].astype(str) + "|" +
    df["Group_EM"].astype(str) + "|" +
    df["Group_EM2"].astype(str) + "|" +
    df["CPT_List_Str"].astype(str)
)

# === Step 6: Export Clean CSV and ZIP ===
df.to_csv(CSV_PATH, index=False)
with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zipf:
    zipf.write(CSV_PATH, arcname=CSV_FILENAME)

print("✅ Preprocessing export complete:")
print(f"    ➤ Clean CSV: {CSV_PATH}")
print(f"    ➤ ZIP archive: {ZIP_PATH}")
print(f"    ➤ Validation report: {VALIDATION_REPORT}")
