import logging
import os
from datetime import datetime


def get_logger(name:str):
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/pipeline_{datetime.now():%Y%m%d}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s| %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )

        fh.setFormatter(fmt)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)
    
    return logger