"""

    postprocess
    ~~~~~~~~~~~

    This module provides postprocessing functions and associated utilities.
    These functions perform the following tasks:

    - Merge judgment entries that refer to the same content together.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-04

"""

import os
import sys
import json
import traceback
import collections

from . import logger
from .. import utils

# ==== Helper Functions

def merge_entries(judgment: dict, entries):
    """ Merges the key values of a judgment object with other judgment objects.
        Each key common across entries becomes a list of values.

    Args:
        judgment (dict): Judgment to merge entries with.
        entries (list[dict]): List of judgment entries to merge.

    Returns:
        dict: The judgment with merged keys.
    """

    common_keys = set(judgment.keys())
    for entry in entries:
        common_keys &= set(entry.keys())
    common_keys -= { 'document_path', 'paragraphs' }

    merged_keys = collections.defaultdict(list)
    for key in common_keys:
        merged_keys[key].extend(utils.as_list(judgment[key]))
        merged_keys[key].extend(
            value for entry in entries
            for value in utils.as_list(entry.get(key))
        )

    judgment.update(merged_keys)
    return judgment

# ==== Main pipeline phase implementation

def merge_judgments(prog, args, judgment_batches, **_):
    """ Post-processing phase: Merge judgment objects together as one. """

    if not args.save_json: return

    print(prog, ": post-processing: merging judgments with same data ...")

    # Merge requests across batches.
    all_requests = collections.defaultdict(list)
    for batch in judgment_batches:
        all_requests = utils.merge_dicts(all_requests, batch['merger_requests'])

    # Group requests with common indexes.
    print("  : grouping judgments as per indices ... ", sep='', end='', flush=True)
    clustered_requests = collections.defaultdict(lambda: collections.defaultdict(list))
    for json_file_path, entries in all_requests.items():
        for entry in entries:
            index, judgment = entry['index'], entry['data']
            clustered_requests[json_file_path][index].append(judgment)
    print("done")

    for json_file_path, merge_dict in clustered_requests.items():
        stem = os.path.basename(json_file_path)
        print("  : updating data in", stem, "... ", end='', flush=True)
        try:
            with open(json_file_path, 'r+', encoding='utf-8') as file:
                data = json.load(file)

                for index, entries in utils.iter_progress(merge_dict.items()):
                    data['data'][index] = merge_entries(data['data'][index], entries)

                file.seek(0)
                json.dump(data, file, indent=4, ensure_ascii=False)
                file.truncate()
                print("\b\bdone")

        except Exception as exc:
            print("\b\berror")
            print(prog, ": error: ", exc, sep='', file=sys.stderr)
            logger.exception("error")
            if args.debug:
                traceback.print_exc()
