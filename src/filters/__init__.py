from .. import logger as root_logger
logger = root_logger.getChild(__name__)

from .base import Filter
from .sent_count_filter import SentenceCountFilter

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "Filter",
    "SentenceCountFilter",
    "logger"
]