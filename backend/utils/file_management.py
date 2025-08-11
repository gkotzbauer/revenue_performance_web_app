import os
import pandas as pd
from datetime import datetime

def save_uploaded_file(file, upload_dir="uploads"):
    """
    Save an uploaded file to the specified directory.
    Returns the file path.
    """
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{file.filename}"
    file_path = os.path.join(upload_dir, filename)

    with open(file_path, "wb") as f:
        f.write(file.read())

    return file_path

def load_csv_file(file_path):
    """
    Load a CSV file into a pandas DataFrame.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)

def load_excel_file(file_path, sheet_name=0):
    """
    Load an Excel file into a pandas DataFrame.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_excel(file_path, sheet_name=sheet_name)

def get_latest_uploaded_file(upload_dir="uploads"):
    """
    Get the most recent file in the upload directory.
    """
    if not os.path.exists(upload_dir):
        return None
    files = [os.path.join(upload_dir, f) for f in os.listdir(upload_dir) if os.path.isfile(os.path.join(upload_dir, f))]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file