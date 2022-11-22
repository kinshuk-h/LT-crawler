import io
import os
import uuid

import pdfminer.layout
import pdfminer.high_level

from . import logger as root_logger
from .base import Extractor

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

class PdfminerHighLevelTextExtractor(Extractor):
    """ Performs text extraction from PDFs using pdfminer's high-level operation functions. """

    def __init__(self, **kwargs):
        """ Initializes the text extractor.

        Args:
            **kwargs: arguments to pass to pdfminer during extraction.
        """
        self.kwargs = kwargs
        if  'laparams' not in self.kwargs:
            self.kwargs.update(laparams=pdfminer.layout.LAParams())

    def output_file(self, pdf_reference: str | io.IOBase, pdf) -> str | list[str]:
        """ Returns the name(s) of output files to generate. """
        if isinstance(pdf_reference, str):
            return os.path.splitext(pdf_reference)[0] + ".txt"
        else:
            return str(uuid.uuid4()) + ".txt"

    def extract(self, pdf):
        """ Extract content from a PDF representation. """
        return pdfminer.high_level.extract_text(pdf, **self.kwargs)
