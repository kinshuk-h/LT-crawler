"""

    paracurate.py
    ~~~~~~~~~~~~~

    Driver Script to execute the curation pipeline according to specified options.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-05

"""

import os
import sys
import json
import timeit
import datetime
import argparse
import traceback
import collections
import concurrent.futures

import src
from src.pipeline import preprocess, search_and_scrape, postprocess
from src import utils, extractors, segregators, filters

logger = src.make_logger(__name__)

if '-d' in sys.argv or '--debug' in sys.argv:
    src.enable_verbose_logs()

avl_extractors  = {
    'pdfminer_text': extractors.PdfminerHighLevelTextExtractor(),
    'parsr'        : extractors.ParsrExtractor(),
    'parsr_custom' : None,
    'adobe_api'    : None
}
avl_filters     = {
    'sent_count': filters.SentenceCountFilter
}
avl_segregators = {
    'pdfminer_text': None,
    'parsr'        : None,
    'parsr_custom' : None,
    'adobe_api'    : segregators.AdobeJSONSegregator
}

# ==== Pipeline Stage 2: Extract Content from Judgments

def extract_task(args):
    extract_output_dir = os.path.join(args.output_dir, utils.fs.pathsafe("extracted_" + args.extractor))
    os.makedirs(extract_output_dir, exist_ok=True)

    extractor = avl_extractors[args.extractor]
    limit = len(args.batch['judgments'])
    width = len(str(limit))
    extract_results = []
    index = args.manager.add(limit=limit, render=True, prefix=f"   {args.extractor[:20]:20}")
    for i, pdf_file in enumerate(args.batch['judgments'], 1):
        try:
            result = extractor.extract_to_file(
                pdf_file, output_dir=extract_output_dir,
                skip_existing=args.skip_existing
            )
            if isinstance(result, str): result = [ result ]
            extract_results.append(result)
        finally:
            args.manager.update(
                index, increment=1, prefix=f"   {args.extractor[:20]:20}",
                suffix=f"({i:{width}} of {limit:{width}})"
            )
    return extract_results

def extract(prog, args, judgment_batches):
    """ Secondary pipeline phase: Extract text from downloaded judgments. """

    # pylint-disable-next-line: invalid-name
    ExtractTaskArgs = collections.namedtuple(
        "ExtractTaskArgs",
        [ 'manager', 'batch', 'extractor', 'output_dir', 'skip_existing' ]
    )

    print(prog, ": extracting text from judgments ...", sep='')
    for i, batch in enumerate(judgment_batches, 1):

        output_dir = os.path.join(
            args.output_dir, args.document_dir,
            f"{batch['params']['court']} Judgments"
        )
        try:
            print("  : extracting from batch #", i, " ...", sep='')
            manager = utils.ProgressBarManager(size=20)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                extract_dirs = executor.map(extract_task, (
                    ExtractTaskArgs(manager, batch, extractor, output_dir, args.skip_existing)
                    for extractor in args.extractors
                ))
                batch['extractions'] = {
                    extractor: extract_dir
                    for extractor, extract_dir in zip(args.extractors, extract_dirs)
                }
                print()
        except Exception as exc:
            print('error', flush=True)
            print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
            if args.debug:
                traceback.print_exc()

    print()
    return judgment_batches

# ==== Pipeline Stage 3: Process Content, De-duplicate

def process(prog, args, judgment_batches, file_index):
    """ Tertiary pipeline phase: process extracted text content. """

    # file_index, _ = data_indexes

    print(prog, ": processing judgments ...", sep='')
    for i, batch in enumerate(judgment_batches, 1):

        try:
            print("  : refiltering existing judgment files for batch #", i," (based on hashes) ... ")
            for extractor in args.extractors:
                unique_indexes = []
                print("    checking extraction results for", extractor, "... ", end='', flush=True)
                extractor_group = utils.fs.pathsafe(f"extracted_{extractor}")

                tic = timeit.default_timer()
                for index in batch.get('indexes', range(len(batch['judgments']))):
                    files = batch['extractions'][extractor][index]
                    for file in files:
                        # TODO: Prepare for merge
                        status, info = file_index.has(file, extractor_group, return_info=True)
                        # print("index", index, ":", file, "in file store?", status and info['match'] != file)
                        if status and info['match'] != file:
                            logger.debug(
                                "collision: %s (new) and %s (present)",
                                os.path.basename(file), os.path.basename(info['match'])
                            )
                            break
                        file_index.load(file, extractor_group, index_info=info)
                    else:
                        unique_indexes.append(index)
                if len(unique_indexes) != len(batch.get('indexes', batch['judgments'])):
                    batch['indexes']     = unique_indexes
                toc = timeit.default_timer()
                print(f"done (~{toc-tic:.3}s) ({len(unique_indexes)}/{len(batch['judgments'])} unique)", flush=True)

            if (indexes := batch.get('indexes', None)) is not None:
                # Delete files which are redundant.
                for file in utils.filter_by_index(batch['judgments'], indexes, inverse=True):
                    os.remove(file)
                for extractor, extractions in batch['extractions'].items():
                    for files in utils.filter_by_index(extractions, indexes, inverse=True):
                        for file in files:
                            os.remove(file)

                # Update entries based on unique indexes.
                batch['judgments'] = list(utils.filter_by_index(batch['judgments'], indexes))
                batch['extractions'] = {
                    extractor: list(utils.filter_by_index(extractions, indexes))
                    for extractor, extractions in batch['extractions'].items()
                }

                if args.save_json:
                    print("    updating filtered results to JSON ... ", end='', flush=True)
                    with open(batch['json'], 'r+', encoding='utf-8') as file:
                        data = json.load(file)
                        index, current, judgments = 0, 0, []
                        for judgment in data['data']:
                            if judgment['document_path'] is not None:
                                if index == indexes[current % len(indexes)]:
                                    judgments.append(judgment)
                                    current += 1
                                # else:
                                #     batch['merger_requests'][] = {
                                #         'index': ,
                                #         'data': judgment
                                #     }
                                index += 1
                            else:
                                judgments.append(judgment)
                        data['data'] = judgments
                        file.seek(0)
                        json.dump(data, file, ensure_ascii=False, indent=4)
                        file.truncate()
                    print("done")
        except Exception as exc:
            print('error', flush=True)
            print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
            if args.debug:
                traceback.print_exc()
    print()

    return judgment_batches

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
    extractor_group.add_argument('-e', '--extractors', nargs='*', default=[ *avl_extractors.keys() ],
                                 choices=[ *avl_extractors.keys() ],
                                 help='extractor(s) to use for mining content from the judgment')
    extractor_group.add_argument('--parsr-config', default=None, help='path to configuration for parsr')
    extractor_group.add_argument('--adobe-credentials', default=None, help='path to credentials file for Adobe API')

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

    if 'parsr_custom' in args.extractors:
        if args.parsr_config is not None:
            with open(args.parsr_config, 'r+', encoding='utf-8') as config:
                avl_extractors['parsr_custom'] = extractors.ParsrExtractor(json.load(config))
        else:
            args.extractors.remove('parsr_custom')

    if 'adobe_api' in args.extractors:
        if args.adobe_credentials is not None:
            avl_extractors['adobe_api'] = extractors.AdobeAPIExtractor(
                credentials_file=args.adobe_credentials
            )
        else:
            args.extractors.remove('adobe_api')

    if not any(query for query in args.queries) and args.court != 'SC':
        print(parser.prog, ": error: specify at least one query", sep='', file=sys.stderr, flush=True)
        sys.exit(1)

    pipeline = utils.Pipeline(
        preprocessing={
            'data_indexes': preprocess.load_indexes
        },
        phases=[
            search_and_scrape.search_and_scrape,
            # extract.extract,
            # process.process,
            # segregate.segregate
        ],
        postprocessing={
            '_': postprocess.merge_judgments
        }
    )
    pipeline.execute(parser.prog, args)

if __name__ == "__main__":
    main()