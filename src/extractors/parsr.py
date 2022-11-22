import io
import os
import uuid
import json
import tempfile

import parsr_client as parsr

from . import logger as root_logger
from .base import Extractor

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

class ParsrExtractor(Extractor):
    """ Performs text and layout extraction from PDFs using the parsr package. """

    DEFAULT_CONFIG = {
        "version": 0.9,
        "extractor": {
            "pdf": "pdfminer",
            "ocr": "tesseract",
            "language": [
                "eng"
            ],
            "credentials": {}
        },
        "cleaner": [
            [
                "header-footer-detection",
                {
                    "ignorePages": [],
                    "maxMarginPercentage": 8,
                    "similaritySizePercentage": 10
                }
            ],
            [
                "words-to-line-new",
                {
                    "modifyAvgWordsSpace": 0,
                    "modifyCommonWordsSpace": 0
                }
            ],
            [
                "reading-order-detection",
                {
                    "minVerticalGapWidth": 5,
                    "minColumnWidthInPagePercent": 15
                }
            ],
            [
                "lines-to-paragraph",
                {
                    "tolerance": 0.25
                }
            ]
        ],
        "output": {
            "granularity": "word",
            "includeMarginals": False,
            "includeDrawings": False,
            "formats": {
                "json": True,
                "text": True,
                "csv": False,
                "markdown": False,
                "pdf": False,
                "simpleJson": True
            }
        }
    }

    def __init__(self, config = None):
        """ Initializes the text extractor.

        Args:
            config: configuration to pass to parsr.
        """
        self.client = parsr.ParsrClient('localhost:3001')
        self.config = config or self.DEFAULT_CONFIG
        fd, self.config_path = tempfile.mkstemp(suffix='.json')
        logger.info("config file: %s", self.config_path)
        with os.fdopen(fd, 'w+', encoding='utf-8') as file:
            json.dump(self.config, file, ensure_ascii=False)

    def output_file(self, pdf_reference: str | io.IOBase, pdf) -> str | list[str]:
        """ Returns the name(s) of output files to generate. """
        prefix = os.path.splitext(pdf_reference)[0] if isinstance(pdf_reference, str) else str(uuid.uuid4())
        return [ prefix+".txt", prefix+".json" ]

    def load_pdf(self, pdf_reference: str | io.IOBase):
        return pdf_reference

    def extract(self, pdf):
        """ Extract content from a PDF representation. """
        result = self.client.send_document(
            file_path=pdf, config_path=self.config_path,
            wait_till_finished=True, silent=False, save_request_id=True
        )
        logger.debug("parsr %s: status = %d", result['file'], result['status_code'])
        return [
            self.client.get_text(),
            json.dumps(self.client.get_json(), ensure_ascii=False, indent=4)
        ]
