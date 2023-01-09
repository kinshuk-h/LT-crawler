"""

    paracurate.py
    ~~~~~~~~~~~~~

    Driver Script to execute the curation pipeline according to specified options.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-05

"""

import sys
import json
import timeit
import datetime
import argparse
import traceback
import collections
import concurrent.futures

import src
from src.pipeline import preprocess, search_and_scrape, extract, process, postprocess
from src import utils, segregators, filters

logger = src.make_logger(__name__)

if '-d' in sys.argv or '--debug' in sys.argv:
    src.enable_verbose_logs()

avl_filters     = {
    'sent_count': filters.SentenceCountFilter
}
avl_segregators = {
    'pdfminer_text': None,
    'parsr'        : None,
    'parsr_custom' : None,
    'adobe_api'    : segregators.AdobeJSONSegregator
}

# ==== Pipeline Stage 4: Segregate & Filter

def segregate_task(args):
    segregator = avl_segregators[args.extractor]
    limit = len(args.batch['judgments'])
    width = len(str(limit))
    paragraphs = []

    index = args.manager.add(limit=limit, render=True, prefix=f"{args.extractor[:20]:20}")

    for i, files in enumerate(args.batch['extractions'][args.extractor], 1):
        try:
            paragraphs.append([ *segregator.segregate_file(files) ])
        finally:
            args.manager.update(
                index, increment=1, prefix=f"   {args.extractor[:20]:20}",
                suffix=f"({i:{width}} of {limit:{width}})"
            )

    return paragraphs

def filter_task(args):
    limit = len(args.paragraphs)
    width = len(str(limit))

    index = args.manager.add(limit=limit, render=True, prefix=f"   {args.extractor[:20]:20}")
    paragraphs = []

    for i, paras in enumerate(args.paragraphs, 1):
        try:
            paragraphs.append(args.filter.evaluate(
                paras, value=lambda x: x['content']
            ))
        finally:
            args.manager.update(
                index, increment=1, prefix=f"   {args.extractor[:20]:20}",
                suffix=f"({i:{width}} of {limit})"
            )
    return paragraphs

def segregate(prog, args, judgment_batches):
    """ Quartenary pipeline phase: Segregate processed text as paragraphs. """

    # pylint-disable-next-line: invalid-name
    SegregateTaskArgs = collections.namedtuple(
        "SegregateTaskArgs",
        [ 'manager', 'batch', 'extractor' ]
    )
    # pylint-disable-next-line: invalid-name
    FilterTaskArgs = collections.namedtuple(
        "FilterTaskArgs",
        [ 'manager', 'paragraphs', 'filter', 'extractor' ]
    )

    print(prog, ": extracting paragraphs from judgments ...", sep='', flush=True)

    for i, batch in enumerate(judgment_batches, 1):
        try:
            print("  : extracting from batch #", i, " ...", sep='', flush=True)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                manager = utils.ProgressBarManager(size=20)
                paragraphs = {
                    extractor: paragraphs
                    for extractor, paragraphs in
                    zip(args.extractors, executor.map(segregate_task, (
                        SegregateTaskArgs(manager, batch, extractor)
                        for extractor in args.extractors
                    )))
                }
                print()
            filter_opts = {}
            for filter_name in args.filters:
                print("    applying ", filter_name, " filter over paragraphs ...", sep='', flush=True)
                _filter = avl_filters[filter_name]()
                _filter.load_options_from_args(args)
                filter_opts[filter_name] = _filter.options
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    manager = utils.ProgressBarManager(size=20)
                    paragraphs = {
                        extractor: paragraphs
                        for extractor, paragraphs in
                        zip(args.extractors, executor.map(filter_task, (
                            FilterTaskArgs(manager, paragraphs[extractor], _filter, extractor)
                            for extractor in args.extractors
                        )))
                    }
                print()
            batch['paragraphs'] = paragraphs
            if args.save_json:
                print("  : saving paragraphs to JSON ... ", sep='', end='', flush=True)
                tic = timeit.default_timer()
                with open(batch['json'], 'r+', encoding='utf-8') as file:
                    data = json.load(file)
                    data['meta']['filters'] = filter_opts
                    index = 0
                    for i in range(len(data['data'])):
                        if data['data'][i]['document_path'] is None: continue
                        data['data'][i]['paragraphs'] = {
                            extractor: paragraphs[extractor][index]
                            for extractor in args.extractors
                        }
                        index += 1
                    file.seek(0)
                    json.dump(data, file, ensure_ascii=False, indent=4)
                    file.truncate()
                toc = timeit.default_timer()
                print("done", f"(~{toc-tic:.3f}s)")
        except Exception as exc:
            print('error', flush=True)
            print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
            if args.debug:
                traceback.print_exc()

    print()
    return judgment_batches

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
    filter_group.add_argument('-f', '--filters', nargs='*', default=[ *avl_filters.keys() ],
                              choices=[ *avl_filters.keys() ],
                              help=(
                                'filters(s) to use for filtering paragraphs from the '
                                'judgment, applied in conjunction')
                             )
    for _filter in avl_filters.values():
        for args, kwargs in _filter.get_option_args():
            filter_group.add_argument(*args, **kwargs)

    args = parser.parse_args()

    if not any(query for query in args.queries) and args.court != 'SC':
        print(parser.prog, ": error: specify at least one query", sep='', file=sys.stderr, flush=True)
        sys.exit(1)

    pipeline = utils.Pipeline(
        preprocessing={
            'data_indexes': preprocess.load_indexes
        },
        phases=[
            search_and_scrape.search_and_scrape,
            extract.extract,
            process.process,
            # segregate.segregate
        ],
        postprocessing={
            '_': postprocess.merge_judgments
        }
    )
    pipeline.execute(parser.prog, args)

if __name__ == "__main__":
    main()
