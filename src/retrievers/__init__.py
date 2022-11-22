from .. import logger as root_logger
logger = root_logger.getChild(__name__)

from .base import JudgmentRetriever
from .supreme_court import SCJudgmentRetriever
from .delhi_high_court import DHCJudgmentRetriever

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "JudgmentRetriever",
    "SCJudgmentRetriever",
    "DHCJudgmentRetriever",
    "utils",
    "logger"
]