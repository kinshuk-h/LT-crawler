"""

    segregate
    ~~~~~~~~~

    This module provides functions for the final stage of the pipeline,
    responsible for:
    - segregating extracted content into document units such as paragraphs.
    - filtering undersirable or low-quality document units
      based on specified criteria.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-06

"""

import sys
import json
import functools
import traceback
import collections
import concurrent.futures

from . import logger
from .. import utils, segregators, filters

# ==== Module Constants

# Dictionary of segregator associations for extractors.
AVAILABLE_SEGREGATORS = {
    'pdfminer_text': None,
    'parsr'        : None,
    'parsr_custom' : None,
    'adobe_api'    : segregators.AdobeJSONSegregator
}
# Dictionary of available filters.
AVAILABLE_FILTERS     = {
    'sent_count': filters.SentenceCountFilter
}

# ==== Type Declarations

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

# ==== Helper Functions

def segregate_task(args: SegregateTaskArgs):
    """ Executes segregation task for a single extractor """

    segregator = AVAILABLE_SEGREGATORS[args.extractor]
    limit = len(args.batch['judgments'])
    width = len(str(limit))
    paragraphs = []

    index = args.manager.add(
        limit=limit, render=True,
        prefix=f"{args.extractor[:20]:20}"
    )

    for i, files in enumerate(args.batch['extractions'][args.extractor], 1):
        try:
            paragraphs.append([ *segregator.segregate_file(files) ])
        finally:
            args.manager.update(
                index, increment=1,
                prefix=f"   {args.extractor[:20]:20}",
                suffix=f"({i:{width}} of {limit:{width}})"
            )

    return paragraphs

def filter_task(args: FilterTaskArgs):
    """ Executes filtering task for a single  """
    limit = len(args.paragraphs)
    width = len(str(limit))

    index = args.manager.add(
        limit=limit, render=True,
        prefix=f"   {args.extractor[:20]:20}"
    )
    paragraphs = []

    for i, paras in enumerate(args.paragraphs, 1):
        try:
            paragraphs.append(args.filter.evaluate(
                paras, value=lambda x: x['content']
            ))
        finally:
            args.manager.update(
                index, increment=1,
                prefix=f"   {args.extractor[:20]:20}",
                suffix=f"({i:{width}} of {limit})"
            )
    return paragraphs

@utils.log_time(logger)
def save_paragraphs(batch, extractors, filter_opts):
    """ Saves paragraphs associated with batch into corresponding JSON file.

    Args:
        batch (dict): Batch containing paragraph data
        extractors (list): List of requested extractors.
        filter_opts(dict): Options used while executing the filters.
    """
    with open(batch['json'], 'r+', encoding='utf-8') as file:
        data = json.load(file)

        data['meta']['filters'] = filter_opts

        index = 0
        for i in range(len(data['data'])):
            if data['data'][i]['document_path'] is None: continue
            data['data'][i]['paragraphs'] = {
                extractor: batch['paragraphs'][extractor][index]
                for extractor in extractors
            }
            index += 1

        file.seek(0)
        json.dump(data, file, ensure_ascii=False, indent=4)
        file.truncate()

# ==== Module Functions

@functools.cache
def get_filter_names():
    """ Returns a list of available retriever names. """
    return tuple(AVAILABLE_FILTERS.keys())

@functools.cache
def get_filters():
    """ Returns a list of available retriever names. """
    return tuple(AVAILABLE_FILTERS.values())

def segregate(prog, args, judgment_batches, **_):
    """ Quartenary pipeline phase: Segregate processed text as paragraphs. """

    print(prog, ": segregating paragraphs from judgments ...", sep='', flush=True)

    # Process all batches one-by-one:
    for i, batch in enumerate(judgment_batches, 1):
        try:
            print("  : segregating from batch #", i, " ...", sep='', flush=True)


            # Execute segregators over extraction results.
            with concurrent.futures.ThreadPoolExecutor() as executor:
                manager = utils.ProgressBarManager(size=20)
                paragraphs = {
                    extractor: paragraphs for extractor, paragraphs in
                    zip(args.extractors, executor.map(segregate_task, (
                        SegregateTaskArgs(manager, batch, extractor)
                        for extractor in args.extractors
                    )))
                }
                print()

            # Execute all filters over the segregation results.
            filter_opts = {}
            for filter_name in args.filters:
                print("    applying ", filter_name, " filter over paragraphs ...", sep='', flush=True)

                _filter = AVAILABLE_FILTERS[filter_name]()
                _filter.load_options_from_args(args)
                filter_opts[filter_name] = _filter.options

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    manager = utils.ProgressBarManager(size=20)
                    paragraphs = {
                        extractor: paragraphs for extractor, paragraphs in
                        zip(args.extractors, executor.map(filter_task, (
                            FilterTaskArgs(manager, paragraphs[extractor], _filter, extractor)
                            for extractor in args.extractors
                        )))
                    }
                print()

            batch['paragraphs'] = paragraphs

            # Save results.
            if args.save_json:
                print("  : saving paragraphs to JSON ... ", sep='', end='', flush=True)
                save_paragraphs(batch, args.extractors, filter_opts)
                print("done")

        except Exception as exc:
            print('error', flush=True)
            print(prog, ": error: ", exc, sep='', file=sys.stderr, flush=True)
            logger.exception("error")
            if args.debug:
                traceback.print_exc()

    print()
    return judgment_batches
