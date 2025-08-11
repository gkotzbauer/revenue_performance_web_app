import pandas as pd
import numpy as np

def to_float_safe(x):
    """
    Safely convert values to float, handling NaN, commas, and percent strings.
    """
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except:
            return np.nan
    try:
        return float(s)
    except:
        return np.nan

def load_data(file_path):
    """
    Loads CSV or Excel into a pandas DataFrame.
    """
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

def normalize_columns(df, percent_cols=None, numeric_cols=None):
    """
    Normalizes percent-like and numeric columns in the DataFrame.
    """
    percent_cols = percent_cols or []
    numeric_cols = numeric_cols or []

    for c in percent_cols:
        if c in df.columns:
            df[c] = df[c].apply(to_float_safe)

    for c in numeric_cols:
        if c in df.columns:
            df[c] = df[c].apply(to_float_safe)

    return df