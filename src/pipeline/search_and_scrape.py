"""

    search_and_scrape
    ~~~~~~~~~~~~~~~~~

    This module provides functions for the primary stage of the pipeline,
    responsible for:

    - Searching judgments on websites of Indian Courts
    - Scraping information pertaining to judgments from search results
    - Downloading judgments for text extraction
    - Saving associated judgment metadata

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-04

"""

import os
import sys
import timeit
import datetime
import traceback
import collections

from .. import utils, retrievers
from . import logger

# ==== Module Constants

# Dictionary of available court website retrievers.
AVAILABLE_RETRIEVERS  = {
    'DHC': retrievers.DHCJudgmentRetriever,
    'SC' : retrievers.SCJudgmentRetriever
}

# ==== Helper Functions

def now(timezone=None):
    """ Returns the current timestamp in ISO 8601 format as a string. """
    timestamp = datetime.datetime.now(tz=timezone).isoformat()
    return timestamp[:timestamp.rfind('.')]

# ==== Module Functions

def get_retriever_names():
    """ Returns a list of available retriever names. """
    return tuple(AVAILABLE_RETRIEVERS.keys())

@utils.log_time(logger)
def deduplicate_judgments(judgment_index, court, judgments, meta_base=None):
    """ Performs the first deduplication step: Remove entries with same
        judgment details (case numbers or URLs).

    Args:
        judgment_index (JudgmentIndexStore): JudgmentIndexStore object for efficient search through objects.
        court (str): Court name to identify the group the file belongs to.
        judgments (list): List of judgment metadata objects to deduplicate.
        meta_base (any, optional): Base metadata to store with unique entries. Defaults to None.

    Returns:
        tuple: Unique judgments, pruning statistics and judgment
            objects to be merged with others.
    """
    unique_judgments, same_case_num_count, same_url_count = [], 0, 0
    merger_requests = collections.defaultdict(list)

    for judgment in judgments:
        item, data = judgment_index.get(judgment, court)
        # If unique judgment, add to index.
        if item is None:
            judgment_index.load(judgment, court, {
                **(meta_base or {}),
                'index': len(unique_judgments)
            })
            unique_judgments.append(judgment)
        else:
            # If same case numbers, ignore new record.
            if item['case_number'] == judgment['case_number']:
                same_case_num_count += 1
            # If same URLs, save record for merging at the end.
            else:
                same_url_count += 1
                merger_requests[data['json']].append({
                    'index': data['index'],
                    'data': judgment
                })

    stats = {
        'same_case_number_count': same_case_num_count,
        'same_url_count'        : same_url_count
    }
    return unique_judgments, stats, merger_requests

@utils.log_time(logger)
def deduplicate_files(file_index, court, judgments, judgment_files, meta_base=None):
    """ Performs the second deduplication step: Remove entries with different
        judgment details but the same document.

    Args:
        file_index (FileIndexStore): FileIndexStore object for efficient search through files.
        court (str): Court name to identify the group the file belongs to.
        judgments (list): List of judgment metadata objects to deduplicate.
        judgment_files (list): List of judgment files to deduplicate.
        meta_base (any, optional): Base metadata to store with unique entries. Defaults to None.

    Returns:
        tuple: Unique judgments, corresponding judgment files and judgment
            objects to be merged with others.
    """
    court_group = f"{court} Judgments"
    unique_judgment_files, unique_judgments = [], []
    merger_requests = collections.defaultdict(list)

    for judgment, file in zip(judgments, judgment_files):
        logger.debug("searching %s in the file index", file)
        data, info = file_index.get(file, court_group, return_info=True)
        if data is None:
            if file is not None:
                file_index.load(file, court_group, index_info=info, meta={
                    **(meta_base or {}),
                    'index': len(unique_judgments)
                })
            unique_judgment_files.append(file)
            unique_judgments.append(judgment)
        else:
            os.remove(file)
            merger_requests[data['json']].append({
                'index': data['index'],
                'data': judgment
            })

    return unique_judgments, unique_judgment_files, merger_requests

# ==== Main pipeline phase implementation

def search_and_scrape(prog, args, data_indexes, **_):
    """ Primary pipeline phase: Search and scrape judgments based on a given
        list of court websites and search parameters. """

    batches = []
    file_index, judgment_index = data_indexes

    # Process queries for each court one-by-one
    for court in args.courts:

        # Create target directories for storing judgments and metadata.
        output_dir = os.path.join(args.output_dir, args.document_dir, f"{court} Judgments")
        json_dir   = os.path.join(args.output_dir, "json", f"{court} Judgments")
        os.makedirs(output_dir, exist_ok=True)
        if args.save_json:
            os.makedirs(json_dir, exist_ok=True)

        retriever = AVAILABLE_RETRIEVERS[court]

        print(prog, ': searching judgments from ', court, ' ... ', sep='', flush=True)
        # Process each query one-by-one
        for query in args.queries:
            num_pages, num_docs, current_page = 0, 0, args.page

            # Process all search results for a given query on a given court, or until specified limits are reached.
            while True:
                try:
                    search_params = { 'query': query, 'page': current_page }

                    json_filestem  = f"{court} {query} page {current_page}"
                    json_file      = f'judgments {utils.fs.pathsafe(json_filestem)}.json'
                    json_file_path = os.path.join(json_dir, json_file)

                    merger_requests = collections.defaultdict(list)

                    print('  : searching using ', ', '.join(f"{key} as {val}" for key,val in search_params.items()),
                          ' ... ', end='', sep='', flush=True)

                    # Skip search when search results exist and option to skip is enabled.
                    if args.skip_existing and os.path.exists(json_file_path):
                        print("skip")
                        print("    skipping search and downloading judgments (files exist)", sep='')

                        data           = utils.fs.read_json(json_file_path)
                        judgments      = data['data']
                        metadata       = data['meta']['response']
                        judgment_files = [
                            os.path.join(output_dir, os.path.basename(judgment['document_path']))
                            for judgment in data['data'] if judgment['document_path'] is not None
                        ]
                    # Otherwise, load results and filter duplicates.
                    else:
                        judgments, metadata = retriever.get_judgments(
                            query, page=current_page,
                            start_date=args.start_date,
                            end_date=args.end_date
                        )

                        if metadata is not None:
                            current_page = metadata['page']
                            search_params['page'] = current_page

                        if not judgments:
                            raise RuntimeError("no judgments found")
                        else: print('done')

                        # Select only those judgments not in the judgment index store.
                        print("  : filtering existing judgment entries (based on case numbers and URLs) ... ", end='')
                        unique_judgments, stats, new_merger_requests = deduplicate_judgments(
                            judgment_index, court, judgments, { 'json': json_file_path }
                        )
                        merger_requests = utils.merge_dicts(merger_requests, new_merger_requests)
                        logger.info(
                            "de-duplication, step 1: total: %d, unique: %d (pruned: %s (case number: %d, URL: %d))",
                            len(judgments), len(unique_judgments), len(judgments) - len(unique_judgments),
                            stats['same_case_number_count'], stats['same_url_count']
                        )
                        judgments = unique_judgments
                        print('done')

                        # Remove entries if the requested limits are reached.
                        if args.limit is not None and num_docs + len(judgments) > args.limit:
                            judgments = judgments[:args.limit-num_docs]

                        # Download judgment files from URLs.
                        print('  : downloading judgments to "', output_dir, '" ... ', sep='', flush=True)
                        tic = timeit.default_timer()
                        judgment_files = retriever.save_documents(
                            judgments, output_dir=output_dir,
                            callback=utils.show_progress(len(judgments))
                        )
                        toc = timeit.default_timer()
                        print(f'done (~{toc-tic:.3}s)', flush=True)

                        # Select only those judgments not in the file index store.
                        print("  : filtering existing judgment files (based on hashes) ... ", end='')
                        unique_judgments, unique_judgment_files, new_merger_requests = deduplicate_files(
                            file_index, court, judgments, judgment_files, { 'json': json_file_path }
                        )
                        merger_requests = utils.merge_dicts(merger_requests, new_merger_requests)
                        logger.info(
                            "de-duplication, step 2: total: %d, unique: %d (pruned: %s)",
                            len(judgments), len(unique_judgments), len(judgments) - len(unique_judgments)
                        )
                        judgments      = unique_judgments
                        judgment_files = list(filter(lambda file: file is not None, unique_judgment_files))
                        print('done')

                    search_params.update({
                        'court'     : court,
                        'start_page': args.page,
                        'req_pages' : args.pages,
                        'req_total' : args.limit,
                        'start_date': args.start_date,
                        'end_date'  : args.end_date,
                    })

                    batch = {
                        'judgments'      : judgment_files,
                        'params'         : search_params,
                        'merger_requests': merger_requests
                    }

                    # Save results if specified.
                    if args.save_json:
                        # Save only when results are newly retrieved.
                        if not os.path.exists(json_file_path) or not args.skip_existing:
                            current_timestamp = now()

                            print('  : saving judgment search results to ', json_file,
                                    ' ... ', sep='', end='', flush=True)
                            result = {
                                'meta': {
                                    'directory': output_dir,
                                    'request'  : search_params,
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
                            utils.fs.write_json(json_file_path, result)
                            print('done')

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
                    logger.exception("error")
                    if args.debug:
                        traceback.print_exc()

                    num_pages    += 1
                    current_page += 1

                # End processing results when page or count limits are reached.
                if current_page is None:
                    break
                if args.limit is not None and num_docs >= args.limit:
                    break
                if args.pages is not None and num_pages >= args.pages:
                    break

    print()
    return batches
