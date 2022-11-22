from .. import logger as root_logger
logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

from .base import Extractor
from .parsr import ParsrExtractor
from .adobe import AdobeAPIExtractor
from .pdfminer import PdfminerHighLevelTextExtractor

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "Extractor",
    "PdfminerHighLevelTextExtractor",
    "ParsrExtractor",
    "AdobeAPIExtractor",
    "logger"
]