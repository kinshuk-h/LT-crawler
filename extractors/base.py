import io
import os
import abc

from .import logger as root_logger

logger = root_logger.getChild(__name__)

class Extractor(abc.ABC):
    """ Abstract class to represent an extractor for extracting content from PDF documents. """

    def load_pdf(self, pdf_reference: str | io.BinaryIO):
        """ Load PDF files into pdf objects or files. """
        if isinstance(pdf_reference, str):
            return open(pdf_reference, "rb", encoding=None)
        else:
            return pdf_reference

    def save_to_file(self, content, path: str):
        """ Saves extracted content to a file.

        Args:
            content (any): Extracted content to be saved.
            path (str): Destination path to save the file.
        """
        with open(path, 'wb', encoding=None) as file:
            file.write(content)

    def extract_to_file(self, pdf_reference: str | io.BinaryIO):
        """ Extracts PDF content to a file.

        Args:
            pdf_reference (str | io.BinaryIO): PDF reference to process and extract from.

        Returns:
            str | list[str]: The file(s) generated post extraction.
        """
        pdf = self.load_pdf(pdf_reference)
        contents = self.extract(pdf)
        _paths    = self.output_file(pdf_reference, pdf)
        paths     = _paths
        if not isinstance(paths, (list, tuple)):
            contents = [ contents ]
            paths    = [ _paths ]
        logger.info("extracting to {} file{}", len(paths), 's' if len(paths)!=1 else '')
        for content, path in zip(contents, paths):
            base = os.path.basename(path)
            logger.debug("extracting content to {}", base)
            self.save_to_file(content, path)
            logger.debug("extracted content to {}", base)
        if isinstance(pdf, io.IOBase):
            pdf.close()
        return _paths

    @abc.abstractmethod
    def output_file(self, pdf_reference: str | io.BinaryIO, pdf) -> str | list[str]:
        """ Returns the name(s) of output files to generate. """
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self, pdf):
        """ Extract content from a PDF representation. """
        raise NotImplementedError