import re

import bs4
import aiohttp
import requests

from . import logger as root_logger
from .base import JudgmentRetriever

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

class DHCJudgmentRetriever(JudgmentRetriever):
    """ Aids in retrieval of judgment documents from the Delhi High Court's website. """
    BASE_URL             = "http://dhcappl.nic.in:8080/FreeText"
    FREE_TEXT_SEARCH_URL = f"{BASE_URL}/GetSearchResult.do"
    SCRIPT_URL_REGEX     = re.compile(r"(?ui)window\.open\('([^']+)',")

    @classmethod
    def make_absolute_url(cls, url):
        if url.startswith('http'): return url
        return f"{cls.BASE_URL}/{url}"

    @classmethod
    def get_judgments(cls, query: str, page: int | str = 0, *args, **kwargs):
        response = requests.post(cls.FREE_TEXT_SEARCH_URL, {
            'search_name': query, 'PAGE_NO': page
        })
        response.raise_for_status()
        logger.debug("%s %s: HTTP %d", response.request.method or "GET", response.url, response.status_code)

        judgments, metadata = [], {}

        page = bs4.BeautifulSoup(response.text, features='lxml')
        if form := page.find('form', attrs={ 'name': 'globalForm' }):
            if tables := form('table', recursive=False):
                table = tables[-1]
                if metacell := table.find('td', colspan='5'):
                    start, end, total = ( int(x) for x in re.findall(r"\d+", metacell.get_text()) )
                    metadata.update(
                        entry_start=start,
                        entry_end=end,
                        entry_total=total,
                        page=int(page.find('input', id='Selected_page')['value']),
                        page_total=total // 10
                    )
                    logger.debug("page %d: %d to %d of %d records", metadata['page'], start, end, total)
                    if start > 1:
                        metadata['page_previous'] = metadata['page']-1
                    if end < total:
                        metadata['page_next'] = metadata['page']+1
                for row in table('tr', bgcolor=True):
                    cells = row('td')
                    judgments.append({
                        'case_number'  : cells[1].string.strip(),
                        'title'        : cells[2].string.strip(),
                        'date'         : cells[3].string.strip(),
                        'document_href': cls.make_absolute_url(cells[4].find('a')['href'])
                    })

        return judgments, metadata

    @classmethod
    async def preprocess_document_url(cls, url: str, session: aiohttp.ClientSession) -> str:
        """ Processes a judgment document URL, resolving it into the actual file URL. """
        if cls.BASE_URL in url:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                if match := cls.SCRIPT_URL_REGEX.search(content):
                    url = match[1]
        return url