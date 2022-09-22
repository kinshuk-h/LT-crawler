import logging

logger = logging.getLogger(__name__)

handler           = logging.StreamHandler()
logging_formatter = logging.Formatter("{asctime} │ {levelname:>8} │ {name}.{funcName}(): {message}", style = '{')
handler.setFormatter(logging_formatter)

logger.addHandler(handler)
logger.setLevel(logging.WARNING)

from .base import Extractor
# from .parsr import ParsrTextExtractor
from .pdfminer import PdfminerHighLevelTextExtractor

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "Extractor",
    "PdfminerHighLevelTextExtractor",
    # "ParsrTextExtractor",
    "logger"
]