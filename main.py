import subprocess
import sys
from datetime import datetime
from src.utils.logger import get_logger


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

    try:
        run_stage("src/pipelines/extract.py")
        run_stage("src/pipelines/transform.py")
        run_stage("src/pipelines/load.py")
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)
    finally:
        logger.info(f"Pipeline FInished ;)")
