import abc

from . import logger as root_logger

logger = root_logger.getChild(__name__)

class Segregator(abc.ABC):
    """ Abstract class to represent a segregator to yield paragraphs from processed files. """

    @classmethod
    @abc.abstractmethod
    def select(cls, files):
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def segregate(cls, data):
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def load(cls, file_path):
        raise NotImplementedError

    @classmethod
    def segregate_file(cls, file):
        if not isinstance(file, str):
            file =  cls.select(file)
        return cls.segregate(cls.load(file))