import os
import re
import sys
import json
import timeit
import logging
import datetime
import argparse
import traceback
import threading
import collections
import concurrent.futures

from src import utils, retrievers, extractors, segregators, filters

if '-d' in sys.argv or '--debug' in sys.argv:
    logging.basicConfig(level=logging.CRITICAL)
    retrievers.logger.setLevel(logging.DEBUG)
    extractors.logger.setLevel(logging.DEBUG)

def now(tz=None):
    """ Returns the current timestamp in ISO 8601 format as a string. """
    timestamp = datetime.datetime.now(tz=tz).isoformat()
    return timestamp[:timestamp.rfind('.')]

def pathsafe(filename):
    """ Returns a santized, path-safe version of a filename. """
    return re.sub(r'[:/\\|*]', '-', re.sub(r'[?\"<>]', '', filename))

def constrain(string, width=30):
    """ Constrain the length of a given string to the specified width. """
    if len(string) > width:
        half_len = len(string) >> 1
        oth_half_len = len(string) - half_len
        string = string[:half_len-1] + "..." + string[oth_half_len-2:]
    return f"{string:{width}}"

def group(files, key=None):
    if key is None:
        key = lambda f: os.path.splitext(os.path.basename(f))[0]
    groups = collections.defaultdict(list)
    for file in files: groups[key(file)].append(file)
    return groups

avl_retrievers  = {
    'DHC': retrievers.DHCJudgmentRetriever,
    'SC' : retrievers.SCJudgmentRetriever
}
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

def show_progress(limit):
    bar = utils.ProgressBar(limit=limit, size=20)
    lock = threading.Lock()
    def show_progress_impl(file_name):
        with lock:
            bar.advance()
            print(f"\r    {constrain(file_name, width=20)}{bar} ", end='')
    return show_progress_impl

def search_and_scrape(prog, args):
    """ Primary pipeline phase: Search and scrape judgments based on a given
        list of court websites and search parameters. """

    batches = []

    for court in args.courts:

        output_dir = os.path.join(args.output_dir, args.document_dir, f"{court} Judgments")
        json_dir = os.path.join(args.output_dir, "json", f"{court} Judgments")
        os.makedirs(output_dir, exist_ok=True)
        if args.save_json:
            os.makedirs(json_dir, exist_ok=True)

        retriever = avl_retrievers[court]

        num_pages, num_docs, current_page = 0, 0, args.page

        print(prog, ': searching judgments from ', court, ' ... ', sep='', flush=True)
        for query in args.queries:
            while True:
                try:
                    search_params = { 'query': query, 'page': current_page }

                    json_filestem = f"{court} {query} page {current_page}"
                    json_file     = f'judgments {pathsafe(json_filestem)}.json'
                    json_file_path = os.path.join(json_dir, json_file)

                    print('  : searching using ', ', '.join(f"{key} as {val}" for key,val in search_params.items()),
                          ' ... ', end='', sep='', flush=True)

                    if args.skip_existing and os.path.exists(json_file_path):
                        print("skip")
                        print("    skipping search and downloading judgments (files exist)", sep='', flush=True)
                        with open(json_file_path, "r", encoding='utf-8') as file:
                            data = json.load(file)
                            judgments = data['data']
                            metadata = data['meta']['response']
                            judgment_files = [
                                os.path.join(output_dir, os.path.basename(judgment['document_path']))
                                for judgment in data['data'] if judgment['document_path'] is not None
                            ]

                    else:
                        judgments, metadata = retriever.get_judgments(
                            query, page=current_page,
                            start_date=args.start_date, end_date=args.end_date
                        )

                        if metadata is not None:
                            current_page = metadata['page']
                            search_params['page'] = current_page

                        if not judgments:
                            print('error', flush=True)
                            print(prog, ": error: no judgments found", sep='', file=sys.stderr, flush=True)
                            continue
                        else: print('done', flush=True)

                        if args.limit is not None and num_docs + len(judgments) > args.limit:
                            judgments = judgments[:args.limit-num_docs]

                        print('  : downloading judgments to "', output_dir, '" ... ', sep='', flush=True)
                        start = timeit.default_timer()
                        judgment_files = retriever.save_documents(
                            judgments, output_dir=output_dir, callback=show_progress(len(judgments))
                        )
                        judgment_files = list(filter(lambda file: file is not None, judgment_files))
                        end   = timeit.default_timer()
                        print(f'done (~{end-start:.3}s)', flush=True)

                    search_params.update({
                        'court'     : court,
                        'start_page': args.page,
                        'req_pages' : args.pages,
                        'req_total' : args.limit,
                        'start_date': args.start_date,
                        'end_date'  : args.end_date,
                    })

                    batch = {
                        'judgments': judgment_files,
                        'params': search_params
                    }

                    if args.save_json:
                        if args.skip_existing and os.path.exists(json_file_path):
                            pass
                        else:
                            current_timestamp = now()

                            print('  : saving judgment search results to ', json_file,
                                    ' ... ', sep='', end='', flush=True)
                            result = {
                                'meta': {
                                    'directory': output_dir,
                                    'request': search_params,
                                },
                                'data': judgments
                            }
                            if metadata is not None:
                                result['meta']['response'] = {
                                    **metadata,
                                    'page_processed': num_pages + 1,
                                    'saved_total'   : num_docs + len(judgments),
                                    'generated_at'  : current_timestamp
                                }
                            with open(json_file_path, 'w+', encoding='utf-8') as file:
                                json.dump(result, file, indent=4, ensure_ascii=False)
                            print('done', flush=True)

                        batch['json'] = json_file_path

                    num_docs += len(judgments)
                    num_pages += 1

                    batches.append(batch)

                    if metadata:
                        current_page = metadata.get('page_next', None)
                    else:
                        current_page += 1

                except Exception as exc:
                    print('error', flush=True)
                    print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
                    traceback.print_exc()

                    num_pages += 1
                    current_page += 1

                finally:
                    if current_page is None: break
                    if args.limit is not None and num_docs >= args.limit: break
                    if args.pages is not None and num_pages >= args.pages: break

    print()
    return batches

def extract_task(args):
    extract_output_dir = os.path.join(args.output_dir, pathsafe("extracted_" + args.extractor))
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
            traceback.print_exc()

    print()
    return judgment_batches

def process(prog, args, judgment_batches):
    """ Tertiary pipeline phase: process extracted text content. """
    # TODO: Implement processing over global content.
    return judgment_batches

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
            for filter_name in args.filters:
                print("    applying ", filter_name, " filter over paragraphs ...", sep='', flush=True)
                _filter = avl_filters[filter_name]()
                _filter.load_options_from_args(args)
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
            traceback.print_exc()

    print()
    return judgment_batches

def main():
    parser = argparse.ArgumentParser(
        description="Collect and extract text from judgments from websites of Indian Courts"
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
    retriever_group.add_argument('-c', '--courts', nargs='*', default=['DHC'], choices=[ *avl_retrievers.keys() ],
                                 help='court website(s) to use to scrape judgments')
    retriever_group.add_argument('-p', '--page', type=int, default=1,
                                 help='starting page number to fetch information from, defaulting to the first')
    retriever_group.add_argument('--limit', type=int, default=None,
                                     help="number of judgments to retrieve, defaulting to as many as available.")
    retriever_group.add_argument('--pages', type=int, default=None,
                                     help="number of pages to retrieve, defaulting to as many as available.")
    retriever_group.add_argument('--start-date', type=datetime.date.fromisoformat,
                                 help='load judgments from this date', default=None, nargs='?')
    retriever_group.add_argument('--end-date', type=datetime.date.fromisoformat,
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

    judgment_batches = search_and_scrape(parser.prog, args)
    judgment_batches = extract          (parser.prog, args, judgment_batches)
    judgment_batches = process          (parser.prog, args, judgment_batches)
    judgment_batches = segregate        (parser.prog, args, judgment_batches)

if __name__ == "__main__":
    main()