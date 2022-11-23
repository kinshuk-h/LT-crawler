import io
import os
import json
import uuid
import time
import logging
import zipfile
import tempfile

import adobe
import regex
from adobe.pdfservices.operation.io.file_ref import FileRef
from adobe.pdfservices.operation.client_config import ClientConfig
from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.execution_context import ExecutionContext
from adobe.pdfservices.operation.pdfops.extract_pdf_operation import ExtractPDFOperation
from adobe.pdfservices.operation.pdfops.options.extractpdf.extract_pdf_options import ExtractPDFOptions
from adobe.pdfservices.operation.pdfops.options.extractpdf.extract_element_type import ExtractElementType
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException

from ..utils import log_time
from . import logger as root_logger
from .base import Extractor

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

class AdobeAPIExtractor(Extractor):
    """ Performs text and layout extraction from PDFs using the Adobe PDF Services API. """

    def __init__(self, credentials_file, max_attempts=3):
        """ Initializes the text extractor and sets up the API context.

        Args:
            credentials_file: JSON file containing credentials as provided by Adobe.
                Ensure the credential file has the correct path to the private key file.
        """
        self.credentials = Credentials.service_account_credentials_builder() \
            .from_file(credentials_file).build()
        self.client_config = ClientConfig.builder() \
            .with_connect_timeout(10000) \
            .with_read_timeout(40000) \
            .build()
        self.context = ExecutionContext.create(self.credentials, self.client_config)
        self.extract_opts = ExtractPDFOptions.builder() \
            .with_element_to_extract(ExtractElementType.TEXT).build()
        self.max_attempts = max_attempts or 3

        # Bind loggers from the Adobe API library with the current library.
        adobe_logger = logging.getLogger(adobe.__name__)
        logger_ref = logger
        while logger_ref:
            if logger_ref.handlers:
                for handler in logger_ref.handlers:
                    adobe_logger.addHandler(handler)
            logger_ref = logger_ref.parent
        adobe_logger.setLevel(logger.getEffectiveLevel())

        # Register a NullHandler to prevent inconsistent logging API usage by the Adobe library.
        logging_root_logger = logging.getLogger()
        if len(logging_root_logger.handlers) == 0:
            logging_root_logger.addHandler(logging.NullHandler())

    @staticmethod
    def save_as_text(pdf_extraction_response, output_file, format_wrt_layout=False):
        elements = pdf_extraction_response['elements']
        current_page, first_write, marker = 0, True, '-' * 20
        total_pages = pdf_extraction_response['extended_metadata']['page_count']
        paragraph_starter_regex = regex.compile(r"(?ui)^\p{Z}*\p{N}+\p{Z}*\.")
        header_path_regex = regex.compile(r"(?u)\/H\d+")

        with open(output_file, 'w+', encoding='utf-8') as file:
            for element in elements:
                if 'Text' in element:
                    if 'Table' in element['Path']:
                        # TODO: Decide how to deal with text elements from tables.
                        continue
                    if format_wrt_layout:
                        is_paragraph_starter = paragraph_starter_regex.match(element['Text'])
                        is_heading = header_path_regex.search(element['Path'])
                        if element['Page'] != current_page and (is_heading or is_paragraph_starter):
                            file.write(f"\n\n{marker} Page {current_page + 1} of {total_pages} end {marker}")
                            current_page += 1
                        if is_heading:
                            file.write(f"\n\n{marker} Heading {marker}")
                        elif is_paragraph_starter:
                            file.write(f"\n\n{marker} Paragraph {marker}")
                    if not first_write:
                        file.write('\n\n')
                    file.write(element['Text'])
                    if first_write:
                        first_write = False
            if format_wrt_layout:
                file.write(f"\n\n{marker} Page {current_page + 1} of {total_pages} end {marker}")

    def output_file(self, pdf_reference: str | io.IOBase, pdf) -> str | list[str]:
        """ Returns the name(s) of output files to generate. """
        prefix = os.path.splitext(pdf_reference)[0] if isinstance(pdf_reference, str) else str(uuid.uuid4())
        return [ prefix+".json", prefix+".txt", prefix+".processed.txt" ]

    def load_pdf(self, pdf_reference: str | io.IOBase):
        if isinstance(pdf_reference, str):
            logger.debug("creating local instance from '%s'", pdf_reference)
            return FileRef.create_from_local_file(pdf_reference)
        else:
            logger.debug("creating local instance from PDF stream")
            return FileRef.create_from_stream(pdf_reference, "application/pdf")

    @log_time(logger)
    def extract(self, pdf):
        """ Extract content from a PDF representation. """
        attempt = 0
        while attempt < self.max_attempts:
            try:
                extract_pdf_operation = ExtractPDFOperation.create_new()
                extract_pdf_operation.set_input(pdf).set_options(self.extract_opts)

                result = extract_pdf_operation.execute(self.context)

                # pylint: disable-next=protected-access
                structured_json, zip_file_path = None, result._file_path
                if zip_file_path is None:
                    zip_file_path = os.path.join(tempfile.gettempdir(), os.urandom(8).hex('-',2)+'.zip')
                    result.save_as(zip_file_path)
                logger.debug(f"extracting content from archive '{zip_file_path}'")
                with zipfile.ZipFile(zip_file_path) as zip_file:
                    structured_json = json.loads(zip_file.read('structuredData.json'))
                if os.path.exists(zip_file_path):
                    os.remove(zip_file_path)

                return structured_json
            except (ServiceApiException, ServiceUsageException, SdkException) as exc:
                logger.exception("%s error while extracting using the API", exc.__class__.__name__)
                if isinstance(exc, ServiceApiException) and "unexpected" in exc.message.lower():
                    return None
                time.sleep(5)
            finally:
                attempt += 1
        return None

    def save_to_file(self, content, path: str):
        """ Saves extracted content to a file.

        Args:
            content (any): Extracted content to be saved.
            path (str): Destination path to save the file.
        """
        if path.endswith('.txt'):
            self.save_as_text(
                content, path,
                format_wrt_layout=path.endswith(".processed.txt")
            )
        elif path.endswith('.json'):
            with open(path, 'w+', encoding='utf-8') as file:
                json.dump(content, file, ensure_ascii=False, indent=4)
        else:
            super().save_to_file(content, path)
