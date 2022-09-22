import io

import pdfminer.layout
import pdfminer.high_level

from . import logger as root_logger
from .base import Extractor

logger = root_logger.getChild(__name__)

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

    def extract(self, ):
                text_file =  + '.txt'
                print('[>] exporting text to', text_file, '... ', end='')
                with open(text_file, 'w+', encoding='utf-8') as txt:
                    pdfminer.high_level.extract_text
                    pdfminer.high_level.extract_text_to_fp(
                        pdf, txt, laparams=pdfminer.layout.LAParams()
                    )
                print('done')

