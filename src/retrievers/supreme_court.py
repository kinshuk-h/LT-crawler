import datetime
from importlib.metadata import metadata

import bs4
import regex
import requests

from . import logger as root_logger
from .base import JudgmentRetriever

logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

COMMA_SEPARATION = regex.compile(r"(?ui)\p{Z}*,\p{Z}*")

def format_date(date_object: datetime.date):
    """ Formats a date in DD-MM-YYYY format. """
    return '-'.join(date_object.isoformat().split('-')[::-1])

class SCJudgmentRetriever(JudgmentRetriever):
    """ Aids in retrieval of judgment documents from the Supreme Court's website. """
    BASE_URL             = "https://main.sci.gov.in"
    ENDPOINTS = {
        'captcha': f"{BASE_URL}/php/captcha_num.php",
        'judgments_by_date': f"{BASE_URL}/php/v_judgments/getJBJ.php",
        'judgments_by_text': f"{BASE_URL}/php/v_judgments/get_Text_Free.php"
    }

    @classmethod
    def generate_captcha(cls):
        """ Generates a fresh captcha string for use in search requests. """
        response = requests.get(cls.ENDPOINTS['captcha'])
        response.raise_for_status()
        return int(response.text)

    @classmethod
    def get_judgments_by_text(cls, captcha, query: str, start_date, end_date):
        """ Return judgments by free text search. """
        search_params = {
            'ansCaptcha': captcha,
            'Free_Text': query,
            'FT_from_date': format_date(start_date),
            'FT_to_date': format_date(end_date)
        }
        logger.debug("request params: %s", ', '.join(f"{key} as {val}" for key,val in search_params.items()))
        response = requests.post(cls.ENDPOINTS['judgments_by_text'], search_params)
        response.raise_for_status()
        logger.debug("%s %s: HTTP %d", response.request.method or "GET", response.url, response.status_code)

        judgments, metadata = [], {}

        page = bs4.BeautifulSoup(response.text, features='lxml')
        if select := page.find('select'):
            if options := select('option'):
                metadata.update(
                    entry_total=len(options),
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat()
                )
                logger.debug("%d records", len(options))
                for item in options:
                    cells = item['value'].split(':')
                    judgments.append({
                        'case_number'  : cells[0].strip(),
                        'title'        : item.get_text().strip(),
                        'date'         : cells[2].strip(),
                        'document_href': f"{cls.BASE_URL}/{cells[3]}"
                    })

        return judgments, metadata

    @classmethod
    def get_judgments_by_date(cls, captcha, start_date, end_date):
        """ Return judgments between a given date range. """
        search_params = {
            'ansCaptcha': captcha,
            'jorrop': 'J',
            'JBJfrom_date': format_date(start_date),
            'JBJto_date': format_date(end_date)
        }
        logger.debug("request params: %s", ', '.join(f"{key} as {val}" for key,val in search_params.items()))
        response = requests.post(cls.ENDPOINTS['judgments_by_date'], search_params)
        response.raise_for_status()

        page = bs4.BeautifulSoup(response.text, features='lxml')
        judgments, metadata = [], {}
        if table := page.find('table'):
            if rows := table('tr', style=False, recursive=False):
                metadata.update(
                    entry_total=len(rows),
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat()
                )
                logger.debug("%d records", len(rows))
                for row in rows:
                    cells = row('td', recursive=False)
                    if len(cells) < 2: continue
                    diary_number = str(cells[2].string).strip()

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    case_number = str(cells[1].string).strip()
                    judgment_url = cells[2]('a', recursive=False)[-1]['href']

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    petitioner_name = str(cells[1].string).strip()

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    respondent_name = str(cells[1].string).strip()

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    petitioner_advocate = str(cells[1].string).strip()

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    respondent_advocate = str(cells[1].string).strip()

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    bench = COMMA_SEPARATION.split(str(cells[1].string))

                    row   = row.find_next_sibling('tr')
                    cells = row('td', recursive=False)
                    judgment_by = str(cells[1].string).strip()

                    judgments.append({
                        'diary_number': diary_number,
                        'case_number': case_number,
                        'by': judgment_by,
                        'bench': bench,
                        'petitioner': {
                            'name': petitioner_name,
                            'advocate': petitioner_advocate,
                        },
                        'respondent': {
                            'name': respondent_name,
                            'advocate': respondent_advocate
                        },
                        'document_href': cls.BASE_URL + judgment_url
                    })

        return judgments, metadata

    @classmethod
    def get_judgments(cls, query: str, start_date=None, end_date=None, *args, **kwargs):
        if end_date is None:
            if start_date is None:
                end_date = datetime.datetime.now().date()
            else:
                end_date = start_date + datetime.timedelta(days=364)
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=364)

        captcha = cls.generate_captcha()

        if query:
            return cls.get_judgments_by_text(captcha, query, start_date, end_date)
        else:
            return cls.get_judgments_by_date(captcha, start_date, end_date)