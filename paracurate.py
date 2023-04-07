"""

    paracurate.py
    ~~~~~~~~~~~~~

    Driver Script to execute the curation pipeline according to specified options.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-05

"""

import sys
import datetime
import argparse

import src
from src import utils
# pylint: disable-next=wildcard-import,unused-wildcard-import
from src.pipeline import *

logger = src.make_logger(__name__)

if '-d' in sys.argv or '--debug' in sys.argv:
    src.enable_verbose_logs()

# ==== Driver: Execute the Pipeline based on given Command-Line Options

def main():
    """  Driver function to parse command-line arguments and execute the pipeline. """
    parser = argparse.ArgumentParser(
        description="Curate a dataset of paragraphs from text from judgments from websites of Indian Courts"
    )
    parser.add_argument('-d', '--debug', action='store_true',
                        help='enable debug logs')
    parser.add_argument('-o', '--output-dir', default='data',
                        help='root directory for storing judgments and metadata')
    parser.add_argument('--skip-existing', action='store_true',
                        help='skip operations if results already exist')
    parser.add_argument('-J', '--omit-json', action='store_false', dest='save_json',
                        help='omit saving judgment list results as JSON')

    retriever_group = parser.add_argument_group(
        "retrieval options", "options to control the search and scrape phase of the pipeline"
    )
    retriever_group.add_argument('queries', help='queries to use for searching judgments', nargs='*', default='')
    retriever_group.add_argument('-c', '--courts', nargs='*', default=['DHC'],
                                 choices=search_and_scrape.get_retriever_names(),
                                 help='court website(s) to use to scrape judgments')
    retriever_group.add_argument('-p', '--page', type=int, default=1,
                                 help='starting page number to fetch information from, defaulting to the first')
    retriever_group.add_argument('--limit', type=int, default=None,
                                     help="number of judgments to retrieve, defaulting to as many as available.")
    retriever_group.add_argument('--pages', type=int, default=None,
                                     help="number of pages to retrieve, defaulting to as many as available.")
    retriever_group.add_argument('--start-date', type=datetime.date.fromisoformat,
                                 help='load judgments from this date', default=None, nargs='?')
    retriever_group.add_argument('--end-date'  , type=datetime.date.fromisoformat,
                                 help='load judgments upto this date', default=None, nargs='?')
    retriever_group.add_argument('--document-dir', default='judgments',
                                 help='output directory to store judgments')

    extractor_group = parser.add_argument_group(
        "extractor options", "options to control the extraction phase of the pipeline"
    )
    extractor_group.add_argument('-e', '--extractors', nargs='*',
                                 default=extract.get_extractor_names(),
                                 choices=extract.get_extractor_names(),
                                 help='extractor(s) to use for mining content from the judgment')
    for extractor in extract.get_extractor_names():
        for args, kwargs in extract.get_option_args(extractor):
            extractor_group.add_argument(*args, **kwargs)

    filter_group = parser.add_argument_group(
        "filter options", "options to control the filtering process"
    )
    filter_group.add_argument('-f', '--filters', nargs='*',
                              default=segregate.get_filter_names(),
                              choices=segregate.get_filter_names(),
                              help=(
                                'filters(s) to use for filtering paragraphs from the '
                                'judgment, applied in conjunction')
                             )
    for _filter in segregate.get_filters():
        for args, kwargs in _filter.get_option_args():
            filter_group.add_argument(*args, **kwargs)

    args = parser.parse_args()

    if not any(query for query in args.queries) and 'SC' not in args.courts:
        print(parser.prog, ": error: specify at least one query", sep='',
              file=sys.stderr, flush=True)
        sys.exit(1)

    pipeline = utils.Pipeline(
        preprocessing={
            'data_indexes': preprocess.load_indexes
        },
        phases=[
            search_and_scrape.search_and_scrape,
            extract.extract,
            process.process,
            segregate.segregate
        ],
        postprocessing={
            '_': postprocess.merge_judgments
        }
    )
    pipeline.execute(parser.prog, args)

if __name__ == "__main__":
    main()
