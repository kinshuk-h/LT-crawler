from .. import logger as root_logger
logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

from .base import Segregator
from .adobe_json import AdobeJSONSegregator

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "Segregator",
    "AdobeJSONSegregator",
    "logger"
]