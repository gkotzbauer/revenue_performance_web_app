import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def compute_ml_performance(y_true, y_pred):
    """
    Compute standard regression metrics for model evaluation.
    Returns a dictionary with MAE, RMSE, and R².
    """
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)

    return {
        "MAE": round(mae, 4),
        "RMSE": round(rmse, 4),
        "R2": round(r2, 4)
    }

def residual_analysis(y_true, y_pred):
    """
    Create a DataFrame with residuals, standardized residuals, and absolute error.
    """
    residuals = y_true - y_pred
    std_residuals = (residuals - np.mean(residuals)) / np.std(residuals)
    abs_error = np.abs(residuals)

    return pd.DataFrame({
        "y_true": y_true,
        "y_pred": y_pred,
        "residuals": residuals,
        "std_residuals": std_residuals,
        "abs_error": abs_error
    })

def feature_importance_summary(model, feature_names):
    """
    Return sorted feature importance values for tree-based models.
    """
    if not hasattr(model, "feature_importances_"):
        raise ValueError("The model does not have feature_importances_ attribute.")

    importance_df = pd.DataFrame({
        "feature": feature_names,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    return importance_df