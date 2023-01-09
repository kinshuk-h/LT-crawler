"""

    pipeline
    ~~~~~~~~

    This module implements the phases of the pipeline as sub-modules.
    Each sub-module exposes the required functionality to execute the
    specific phase of the pipeline.

"""

from .. import logger as root_logger
logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

# pylint: disable-next=wrong-import-position
from . import preprocess, search_and_scrape
# pylint: disable-next=wrong-import-position
from . import extract, process, segregate, postprocess

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "preprocess",
    "search_and_scrape",
    "extract",
    "process",
    "segregate",
    "postprocess",
    "logger"
]
