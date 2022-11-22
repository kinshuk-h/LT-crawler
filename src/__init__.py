import os
import logging
import datetime

logger = logging.getLogger(__name__)

handler = logging.StreamHandler()
handler.setLevel(logging.WARNING)

LOG_DIR = os.path.join("data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, f"{datetime.datetime.now().isoformat('_', timespec='seconds').replace(':','-')}.log"),
    "a+", encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)

logging_formatter = logging.Formatter("{asctime} │ {levelname:>8} │ {funcName}(): {message}", style = '{')
handler.setFormatter(logging_formatter)
file_handler.setFormatter(logging_formatter)

logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.addHandler(file_handler)

from . import utils, retrievers, extractors, segregators, filters

__all__ = [ "utils", "retrievers", "extractors", "segregators", "filters" ]
__author__ = "Kinshuk Vasisht"
__version__ = "1.0.0"