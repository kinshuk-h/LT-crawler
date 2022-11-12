import abc
import argparse
import concurrent.futures

from . import logger as root_logger

logger = root_logger.getChild(__name__)

class Filter(abc.ABC):
    """ Abstract class to represent a filter for selecting paragraphs. """

    name = "base"

    def __init__(self) -> None:
        super().__init__()
        self.options = {}

    @classmethod
    def get_option_args(cls) -> list[tuple[list, dict]]:
        """ Gets a list of supported arguments as argparse compatible
            dictionaries for use in argparse.Parser.add_argument """
        options = cls.get_option_list()
        arg_opts = []
        for option in options:
            option['dest'] = (cls.name + "_" + option['name']).replace('-','_')
            del option['name']
            arg_opts.append(([ '--'+option['dest'].replace('_','-') ], option))
        return arg_opts

    @classmethod
    @abc.abstractmethod
    def get_option_list(cls) -> list[dict]:
        """ Returns a list of dictionaries for supported options.
            Each dictionary describes options with descriptions, values and defaults,
            usable by argparse for registering command-line options.

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError

    def set_options(self, options: dict = None):
        """ Set dynamic filtering options for this filter.

        Args:
            options (dict, optional): Dictionary of option key-value pairs. Defaults to None.
        """
        if options is not None:
            self.options.update(options)
            self.refresh_state()

    def load_options_from_args(self, args: argparse.Namespace):
        """ Load options from an argparse Namespace variable.

        Args:
            args (argparse.Namespace): Namespace containing parsed arguments.
        """
        options = {
            key[len(self.name)+1:] : value
            for key, value in vars(args).items()
            if key.startswith(self.name)
        }
        self.set_options(options)

    @abc.abstractmethod
    def refresh_state(self):
        """ Refresh the internal variables of the filter, owing to a change in the filter options.

        Raises:
            NotImplementedError: Abstract method, to be implemented by subclasses.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, paragraph: str):
        """ Converts a paragraph to an internally suitable representation, such as a list of sentences.

        Args:
            paragraph (str): The paragraph to load, usually a string object.

        Raises:
            NotImplementedError: Abstract method to be implemented by subclasses.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def decision(self, paragraph_rep):
        """ Returns a boolean indicating whether the filter accepts or rejects the paragraph.

        Args:
            paragraph_rep (Any): Internal representation of a paragraph.

        Raises:
            NotImplementedError: Abstract method to be implemented by subclasses.
        """
        raise NotImplementedError

    def evaluate(self, paragraphs: list, value = None) -> list[str]:
        """ Filters paragraphs from a list of paragraphs.

        Args:
            paragraphs (list): The list of paragraphs to evaluate.
            value((Any) -> str): Function to transform paragraph objects to string.

        Returns:
            list[Any]: List of accepted paragraphs.
        """

        with concurrent.futures.ThreadPoolExecutor() as executor:
            if value is not None:
                paras = executor.map(value, paragraphs)
            else:
                paras = paragraphs
            reps = executor.map(self.load, paras)
            return [
                para for para, decision in
                zip(paragraphs, executor.map(self.decision, reps)) if decision
            ]
