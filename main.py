import subprocess
import sys
import os
from datetime import datetime
from src.utils.logger import get_logger

sys.path.append(os.path.abspath(os.getcwd()))
os.environ["PYTHONPATH"] = os.path.abspath(os.getcwd())


logger = get_logger("Main Data Pipeline")

def run_stage(script_path:str):
    logger.info(f"Started Executing {script_path}...")

    start_time =datetime.now()

    result = subprocess.run([sys.executable, script_path])
    duration = (datetime.now() - start_time).total_seconds()

    if result.returncode != 0:
        logger.error(f"Stage failed : {script_path}, Duration: {duration:.1f}s")
        raise RuntimeError(f"Stage failed: {script_path}")
    else:
        logger.info(f"Completed : {script_path}, Duration:{duration:.2f}s")
    

if __name__ == "__main__":
    logger.info(f"TransitX Data Pipeline Start | {datetime.now():%Y-%m-%d %H:%M:%S}")

    stages = [
        run_stage("src/pipelines/extract.py"),
        run_stage("src/pipelines/transform.py"),
        run_stage("src/pipelines/feature_eng.py"),
        run_stage("src/pipelines/load.py")
    ]

    try:
        for stage in stages:
            run_stage(stage)
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)
    finally:
        logger.info(f"Pipeline FInished ;)")
