import subprocess
import os
import sys
import logging
from datetime import datetime

# =========================================
# Pipeline Utilities
# =========================================

def run_script(script_path, env_overrides=None, cwd=None, log_file=None):
    """
    Runs a Python script as a subprocess, streams stdout/stderr to console and log file.
    Returns True if successful, False otherwise.
    """
    if not os.path.isfile(script_path):
        logging.error(f"❌ Missing script: {script_path}")
        return False

    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)

    cmd = [sys.executable, script_path]

    logging.info(f"▶ Running: {script_path}")
    try:
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=cwd,
            universal_newlines=True
        ) as proc:

            for line in proc.stdout:
                print(line, end="")
                if log_file:
                    with open(log_file, "a") as lf:
                        lf.write(line)

            proc.wait()
            if proc.returncode != 0:
                logging.error(f"❌ {script_path} failed with return code {proc.returncode}")
                return False

        logging.info(f"✅ Completed: {script_path}")
        return True

    except Exception as e:
        logging.exception(f"❌ Exception running {script_path}: {e}")
        return False


def run_pipeline(scripts, start_at=None, env_overrides=None, log_dir="logs"):
    """
    Runs the pipeline scripts in sequence with optional start point.
    Logs are saved to logs/pipeline_TIMESTAMP.log
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )

    logging.info("🚀 Starting Pipeline Run")
    logging.info(f"Logs will be saved to: {log_file}")

    start_found = start_at is None
    for label, script in scripts:
        if not start_found and start_at.lower() in label.lower():
            start_found = True

        if start_found:
            success = run_script(script, env_overrides=env_overrides, log_file=log_file)
            if not success:
                logging.error(f"❌ Stopping pipeline due to failure at step: {label}")
                break
        else:
            logging.info(f"⏭ Skipping: {label}")

    logging.info("🎉 Pipeline completed.")


def validate_required_columns(df, required_cols, step_name):
    """
    Checks if all required columns are present in DataFrame.
    Returns (True, []) if all present, (False, missing_list) otherwise.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logging.warning(f"⚠️ {step_name}: Missing columns: {missing}")
        return False, missing
    return True, []