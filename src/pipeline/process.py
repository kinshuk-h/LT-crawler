"""

    process
    ~~~~~~~

    This module provides functions for the third stage of the pipeline,
    responsible for:
    - sanitizing extracted content of minor irregularities.
    - de-duplication step where judgments whose extracted content
      are the same are identified and merged.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-06

"""

import os
import sys
import traceback
import collections

from . import logger
from .. import utils

# ==== Helper Functions

@utils.log_time(logger)
def deduplicate_by_content(file_index, extractor, batch, judgments, judgment_indexes=None):
    """ Performs the third deduplication step: Remove entries with different
        judgment files but the same content.

    Args:
        file_index (FileIndexStore): FileIndexStore object for efficient search through files.
        extractor (str): Extractor name to identify the group the file belongs to.
        batch (dict): Batch the set of judgments belong to.
        judgments (list): List of judgment metadata objects to deduplicate.
        judgement_indexes (list|None): List of indexes to filter judgments.

    Returns:
        tuple: Unique indexes for judgments, unique indexes for judgment files
            and judgment objects to be merged with others.
    """
    meta_base = { 'json': batch.get('json') }

    doc_ptr, unique_indexes, unique_judgment_indexes = 0, [], []
    extractor_group = utils.fs.pathsafe(f"extracted_{extractor}")
    indexes = batch.get('indexes', range(len(batch['judgments'])))
    judgment_indexes = judgment_indexes or range(len(judgments))
    merger_requests = collections.defaultdict(list)

    for index in indexes:
        # Advance to the first document index with a document path
        while doc_ptr < len(judgment_indexes):
            doc_index = judgment_indexes[doc_ptr]
            if judgments[doc_index].get('document_path') is not None: break
            unique_judgment_indexes.append(doc_index)
            doc_ptr += 1
        print(index, doc_ptr, judgment_indexes[doc_ptr])

        files, infos = batch['extractions'][extractor][index], []
        for file in files:
            data, info = file_index.get(file, extractor_group, return_info=True)
            if data is not None and info['match'] != file:
                # Collision with existing file, merge referred judgment
                logger.debug(
                    "collision: %s (new) and %s (present)",
                    os.path.basename(file), os.path.basename(info['match'])
                )
                merger_requests[data['json']].append({
                    'data': judgments[judgment_indexes[doc_ptr]],
                    'index': data['index']
                })
                break
            # Store info for future load operation to index
            infos.append(info)
        else:
            # No collisions, add all files to index store
            for file, info in zip(files, infos):
                file_index.load(file, extractor_group, index_info=info, meta={
                    **(meta_base or {}), 'index': judgment_indexes[doc_ptr]
                })
            unique_indexes.append(index)
            unique_judgment_indexes.append(judgment_indexes[doc_ptr])
        doc_ptr += 1

    # Add remaining judgments without document paths
    while doc_ptr < len(judgment_indexes):
        doc_index = judgment_indexes[doc_ptr]
        unique_judgment_indexes.append(doc_index)
        doc_ptr += 1

    return unique_judgment_indexes, unique_indexes, merger_requests

# ==== Module Functions

def process(prog, args, judgment_batches, data_indexes, **_):
    """ Tertiary pipeline phase: process extracted text content. """

    file_index, _ = data_indexes

    # Process each batch one-by-one:
    print(prog, ": processing judgments ...", sep='')
    for i, batch in enumerate(judgment_batches, 1):

        data = {}
        try:
            if args.save_json:
                data = utils.fs.read_json(batch['json'])
            judgments = data.get('data', [])
            judgment_indexes = None

            # Process each extractor's results per batch and select unique documents across all
            print("  : refiltering existing judgment files for batch #", i," (based on hashes) ... ")
            for extractor in args.extractors:
                print("    checking extraction results for", extractor, "... ", end='', flush=True)

                # Generate indices to select only records with unique extraction results.
                unique_judgment_indexes, batch['indexes'], merger_requests = deduplicate_by_content(
                    file_index, extractor, batch, judgments, judgment_indexes
                )
                batch['merger_requests'] = utils.merge_dicts(batch['merger_requests'], merger_requests)
                judgment_indexes = unique_judgment_indexes

            if (indexes := batch.get('indexes', None)) is not None:
                # Delete files which are redundant.
                for file in utils.filter_by_index(batch['judgments'], indexes, inverse=True):
                    os.remove(file)
                for extractor, extractions in batch['extractions'].items():
                    for files in utils.filter_by_index(extractions, indexes, inverse=True):
                        for file in files:
                            os.remove(file)

                # Update entries based on unique indexes.
                batch['judgments']   = list(utils.filter_by_index(batch['judgments'], indexes))
                batch['extractions'] = {
                    extractor: list(utils.filter_by_index(extractions, indexes))
                    for extractor, extractions in batch['extractions'].items()
                }

                if args.save_json:
                    unique_judgments = utils.filter_by_index(judgments, judgment_indexes)
                    logger.info(
                        "de-duplication, step 3: total: %d, unique: %d (pruned: %s)",
                        len(judgments), len(unique_judgments), len(judgments) - len(unique_judgments)
                    )
                    print("    updating filtered results to JSON ... ", end='', flush=True)
                    data['data'] = judgments
                    utils.fs.write_json(batch['json'], data)
                    print("done")

        except Exception as exc:
            print('error', flush=True)
            print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
            logger.debug("error")
            if args.debug:
                traceback.print_exc()
    print()

    return judgment_batches
