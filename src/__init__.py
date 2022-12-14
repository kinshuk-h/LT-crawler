import os
import logging
import datetime

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.CRITICAL)

LOG_DIR = os.path.join("data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, f"{datetime.datetime.now().isoformat('_', timespec='seconds').replace(':','-')}.log"),
    "a+", encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)

simple_formatter  = logging.Formatter("<> {levelname:>8}: {funcName}(): {message}", style = '{')
console_handler.setFormatter(simple_formatter)
logging_formatter = logging.Formatter(
    "{asctime} │ {levelname:>8} │ {name}.{funcName}:{lineno}: {message}", style = '{'
)
file_handler.setFormatter(logging_formatter)

logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

from . import utils, retrievers, extractors, segregators, filters

def enable_verbose_logs():
    """ Enables logging over the stream STDERR handler. """
    console_handler.setLevel(logging.DEBUG)

def disable_verbose_log():
    """ Disables logging over the stream STDERR handler. """
    console_handler.setLevel(logging.CRITICAL)

def make_logger(name):
    """ Creates a new logger and binds previously initialized handlers. """
    logger = logging.getLogger(name.rsplit('.', maxsplit=1)[-1])
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger

__all__ = [ "utils", "retrievers", "extractors", "segregators", "filters" ]
__author__ = "Kinshuk Vasisht"
__version__ = "1.0.0"