import logging

logger = logging.getLogger(__name__)

handler           = logging.StreamHandler()
logging_formatter = logging.Formatter("{asctime} │ {levelname:>8} │ {funcName}(): {message}", style = '{')
handler.setFormatter(logging_formatter)

logger.addHandler(handler)
logger.setLevel(logging.WARNING)

from .base import Filter
from .sent_count_filter import SentenceCountFilter

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "Filter",
    "SentenceCountFilter",
    "logger"
]