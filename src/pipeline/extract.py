"""

    extract
    ~~~~~~~

    This module provides functions for the second stage of the pipeline,
    responsible for extracting text from downloaded judgments using various
    implemented extractors.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-06

"""

import os
import sys
import functools
import traceback
import collections
import concurrent.futures

from . import logger
from .. import utils, extractors

# ==== Module Constants

# Dictionary of available extractors.
AVAILABLE_EXTRACTORS  = {
    'pdfminer_text': extractors.PdfminerHighLevelTextExtractor,
    'parsr'        : extractors.ParsrExtractor,
    'parsr_custom' : extractors.ParsrExtractor,
    'adobe_api'    : extractors.AdobeAPIExtractor
}

# Dictionary of extractor initialization options.
EXTRACTOR_OPTIONS = {
    'pdfminer_text': None,
    'parsr'        : None,
    'parsr_custom' : [
        dict(name='config_path', argument='parsr-config',
             help='path to configuration for parsr')
    ],
    'adobe_api'    : [
        dict(name='credentials_file', argument='adobe-credentials',
             help='path to credentials file for Adobe API')
    ]
}

# ==== Type Declarations

# pylint-disable-next-line: invalid-name
ExtractTaskArgs = collections.namedtuple(
    "ExtractTaskArgs",
    (
        'manager', 'batch', 'extractor_instance',
        'extractor', 'output_dir', 'skip_existing'
    )
)

# ==== Helper Functions

def extract_task(args: ExtractTaskArgs):
    """ Executes extractions task for a single extractor. """

    extract_output_dir = os.path.join(
        args.output_dir, utils.fs.pathsafe("extracted_" + args.extractor)
    )
    os.makedirs(extract_output_dir, exist_ok=True)

    extractor = args.extractor_instance
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

# ==== Module Functions

def get_option_args(extractor):
    """ Gets a list of supported arguments as argparse compatible
        dictionaries for use in argparse.Parser.add_argument

    Args:
        extractor (str): Extractor to retrieve options list for.

    Returns:
        list[tuple[list, dict]]: List of options args and kwargs.
    """
    options = EXTRACTOR_OPTIONS[extractor]
    arg_opts = []
    for option in (options or []):
        option = option.copy()
        option['dest'] = (extractor + "_" + option['name']).replace('-','_')
        del option['name']
        argument_name = option.get('argument')
        if argument_name    : del option['argument']
        if not argument_name: argument_name = option['dest']
        arg_opts.append(([ '--'+argument_name.replace('_','-') ], option))
    return arg_opts

def initialize_extractors(args):
    """ Initializes extractor instances using given arguments. """

    initialized_extractors = {}
    for extractor in args.extractors:
        init_opts = {}
        for option in (EXTRACTOR_OPTIONS[extractor] or []):
            target_option_name = f"{extractor}_{option['name']}"
            if option.get('required', False) or option.get('default') is None:
                if getattr(args, target_option_name, None) is None: break
            init_opts[option['name']] = getattr(args, target_option_name)
        else:
            initialized_extractors[extractor] = AVAILABLE_EXTRACTORS[extractor](**init_opts)
    return initialized_extractors

@functools.cache
def get_extractor_names():
    """ Returns a list of available retriever names. """
    return tuple(AVAILABLE_EXTRACTORS.keys())

def extract(prog, args, judgment_batches, **_):
    """ Secondary pipeline phase: Extract text from downloaded judgments. """

    # Initialize custom argument extractors:
    available_extractors = initialize_extractors(args)
    args.extractors = list(available_extractors.keys())
    logger.debug("available initialized extractors: %s", ','.join(args.extractors))

    # Process each batch one by one:
    print(prog, ": extracting text from judgments ...", sep='')
    for i, batch in enumerate(judgment_batches, 1):

        output_dir = os.path.join(
            args.output_dir, args.document_dir,
            f"{batch['params']['court']} Judgments"
        )

        try:
            # Execute all extractors concurrently:
            print("  : extracting from batch #", i, " ...", sep='')
            manager = utils.ProgressBarManager(size=20)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                extract_dirs = executor.map(extract_task, (
                    ExtractTaskArgs(
                        manager, batch, available_extractors[extractor],
                        extractor, output_dir, args.skip_existing)
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
            logger.exception("error")
            if args.debug:
                traceback.print_exc()

    print()
    return judgment_batches
