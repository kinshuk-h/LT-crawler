import abc
import asyncio

import aiohttp

from .utils import download_file

class JudgmentRetriever(abc.ABC):
    """ Abstract class to group methods related to retrieval of judgment documents from court websites. """
    @classmethod
    @abc.abstractmethod
    def get_judgments(cls, query: str, page: int|str = 0) -> tuple[list[dict[str]], dict[str]]:
        """ Abstract method to retrieve judgment details for a given search query. """
        raise NotImplementedError

    @classmethod
    async def preprocess_document_url(cls, url: str, session: aiohttp.ClientSession = None) -> str:
        """ Preprocesses the document URL, resolving any intermediate pages to the final click-to-download URL. """
        return url

    @classmethod
    async def __save_documents_impl(cls, judgments: list[dict[str]], output_dir: str = "."):
        """ Downloads judgment documents asynchronously and saves them to a specified directory.
            This method is an asynchronous implementation for downloading multiple files concurrently.

        Args:
            judgments (list[dict[str]]): Judgment objects, as returned by a call to `get_judgments`
            output_dir (str, optional): Directory to save documents in. Defaults to the current working directory.
        """
        async with aiohttp.ClientSession() as session:
            # Get download URLs for every document:
            urls = await asyncio.gather(*(
                cls.preprocess_document_url(judgment['document_href'], session)
                for judgment in judgments
            ))
            # Download and return the paths to downloaded files:
            paths = await asyncio.gather(*(
                download_file(url, session=session, output_dir=output_dir, suppress_exc=True)
                for url in urls
            ))
            # Update judgment objects with the downloaded paths, and return the paths:
            for path, judgment in zip(paths, judgments):
                judgment['document_path'] = path
            return paths

    @classmethod
    def save_documents(cls, judgments: list[dict[str]], output_dir: str = "."):
        """ Downloads judgment documents asynchronously and saves them to a specified directory.

        Args:
            judgments (list[dict[str]]): Judgment objects, as returned by a call to `get_judgments`
            output_dir (str, optional): Directory to save documents in. Defaults to the current working directory.
        """
        return asyncio.run(cls.__save_documents_impl(judgments, output_dir=output_dir))