import io
import os
import abc
import itertools

from . import logger as root_logger

logger = root_logger.getChild(__name__)

class Extractor(abc.ABC):
    """ Abstract class to represent an extractor for extracting content from PDF documents. """

    def load_pdf(self, pdf_reference: str | io.IOBase):
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
            file.write(content.encode() if isinstance(content, str) else content)

    def extract_to_file(self, pdf_reference: str | io.IOBase, output_dir=None, skip_existing=False):
        """ Extracts PDF content to a file.

        Args:
            pdf_reference (str | io.IOBase): PDF reference to process and extract from.
            output_dir (str|None, optional): Destination directory for extracted files.
                Defaults to the same as the PDF, if a path is given, otherwise the CWD.
            skip_existing (bool, optional): If true, skips processing existing extracted files.

        Returns:
            str | list[str]: The file(s) generated post extraction.
        """
        pdf = self.load_pdf(pdf_reference)
        _paths    = self.output_file(pdf_reference, pdf)
        paths     = _paths
        if not isinstance(paths, (list, tuple)):
            paths    = [ _paths ]
        for i, path in enumerate(paths):
            base = os.path.basename(path)
            if output_dir is not None:
                path = os.path.join(output_dir, base)
            paths[i] = ( base, path )
        _paths = paths[0][1] if len(paths) == 1 else [ path[1] for path in paths ]

        # If set to skip existing, skip existing.
        if skip_existing and any(os.path.exists(path) for _, path in paths):
            return _paths

        # Otherwise, extract content and save files.
        contents = self.extract(pdf)
        if not isinstance(contents, (list, tuple)):
            contents = [ contents ]
        logger.info("extracting to %d file%s", len(paths), 's' if len(paths)!=1 else '')
        for content, (base, path) in zip(itertools.cycle(contents), paths):
            logger.debug("extracting content to %s", base)
            self.save_to_file(content, path)
            logger.debug("extracted content to %s", base)
        if isinstance(pdf, io.IOBase):
            pdf.close()
        return _paths

    @abc.abstractmethod
    def output_file(self, pdf_reference: str | io.IOBase, pdf) -> str | list[str]:
        """ Returns the name(s) of output files to generate. """
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self, pdf):
        """ Extract content from a PDF representation. """
        raise NotImplementedError