import timeit
import logging
import threading
import functools

from .. import logger as root_logger
_logger = root_logger.getChild(__name__.rsplit('.', maxsplit=1)[-1])

from . import fs
from .progress import ProgressBar, IndeterminateProgressCycle, ProgressBarManager

def constrain(string, width=30):
    """ Constrain the length of a given string to the specified width. """
    if len(string) > width:
        half_len = width >> 1
        oth_half_len = width - half_len
        string = string[:half_len-1] + "..." + string[-oth_half_len+2:]
    return f"{string:{width}}"

def log_time(logger: logging.Logger, level = logging.INFO):
    """ Generates a decorator that logs the execution time of a function to a given logging.Logger object. """
    def make_time_logger(func):
        @functools.wraps(func)
        def call_and_log_time(*args, **kwargs):
            tic = timeit.default_timer()
            result = func(*args, **kwargs)
            toc = timeit.default_timer()
            logger.log(level, "%s(): execution completed in %.3fs", func.__name__, toc-tic)
            return result
        return call_and_log_time
    return make_time_logger

def filter_by_index(values, indexes, inverse=False):
    """ Filters a list of values based on indices to select.

    Args:
        values (list): An iterable collection to filter.
        indexes (Iterable): Iterable collection (sorted) of indexes to select.
        inverse (bool, optional): If true, returns the elements whose indexes are NOT specified. Defaults to False.

    Returns:
        Generator: Generator for filtered values
    """
    if inverse:
        iterator = iter(indexes)
        current = next(iterator, None)
        for index, value in enumerate(values):
            if current is not None and index == current:
                current = next(iterator, None)
            else:
                yield value
    else:
        yield from ( values[index] for index in indexes )

def show_progress(limit):
    """ Callback curry for indicating progress across a set of related async tasks. """
    progress_bar = ProgressBar(limit=limit, size=20)
    lock         = threading.Lock()
    def show_progress_impl(file_name):
        with lock:
            progress_bar.advance()
            print(f"\r    {constrain(file_name, width=20)}{progress_bar} ", end='')
    return show_progress_impl

def show_indeterminate_progress():
    """ Callback curry for indeterminate progress across a set of related async tasks. """
    indet_bar = IndeterminateProgressCycle()
    lock      = threading.Lock()
    print("  ", end='', flush=True)
    def show_progress_impl(*_):
        with lock:
            indet_bar.advance()
            print(f"\b\b{indet_bar} ", end='', flush=True)
    return show_progress_impl

def iter_progress(iterable):
    """ Generator wrapper for indeterminate progress while iterating over an iterable. """
    progress_callback = show_indeterminate_progress()
    for value in iterable:
        yield value
        progress_callback()

def merge_dicts(dict1, dict2):
    """ Merges two dictionaries into one, extending the keys upon clash. """
    merged_dict = dict1.copy()
    for key, value in dict2.items():
        if not isinstance(value, (list, tuple)): value = ( value, )
        old_value = merged_dict.get(key, [])
        if not isinstance(old_value, list): old_value = [ old_value ]
        old_value.extend(value)
        merged_dict[key] = old_value
    return merged_dict

class Pipeline:
    """ Utility class to run multiple functions in a sequential fashion as a pipeline. """

    def __init__(self, phases, preprocessing=None, postprocessing=None) -> None:
        self.preprocessing = preprocessing or {}
        self.postprocessing = postprocessing or {}
        self.stages = phases

    def execute(self, *args, with_preprocessing_results=False, **kwargs):
        """ Execute the pipeline, running all preprocessing and processing stages sequentially. """

        # Execute pre-processing stages:
        preprocessing_results = {}
        for name, preprocessing_fx in self.preprocessing.items():
            _logger.debug(
                "executing pre-processing stage '%s': generate '%s'",
                preprocessing_fx.__name__, name
            )
            tic = timeit.default_timer()
            preprocessing_results[name] = preprocessing_fx(*args, **kwargs)
            toc = timeit.default_timer()
            _logger.info(
                "pre-processing stage '%s': execution completed in %.6gs",
                preprocessing_fx.__name__, toc-tic
            )

        # Execute processing stages:
        result = None
        for i, stage_fx in enumerate(self.stages, 1):
            _logger.debug("executing stage %d/%d: '%s'", i, len(self.stages), stage_fx.__name__)
            tic = timeit.default_timer()
            result = stage_fx(*args, **kwargs, **preprocessing_results)
            toc = timeit.default_timer()
            _logger.info("'%s' stage: execution completed in %.6gs", stage_fx.__name__, toc-tic)
            # Add results to the positional arguments for the next stage.
            args = ( *(args[:-1] if i != 1 else args), result )

        # Execute pre-processing stages:
        postprocessing_results = {}
        for name, postprocessing_fx in self.postprocessing.items():
            _logger.debug(
                "executing post-processing stage '%s': generate '%s'",
                postprocessing_fx.__name__, name
            )
            tic = timeit.default_timer()
            postprocessing_results[name] = postprocessing_fx(
                *args, **kwargs, **preprocessing_results
            )
            toc = timeit.default_timer()
            _logger.info(
                "post-processing stage '%s': execution completed in %.6gs",
                postprocessing_fx.__name__, toc-tic
            )

        # Return the final result
        results = [ result, postprocessing_results ]
        if with_preprocessing_results:
            results.insert(0, preprocessing_results)
        return results

def as_list(value):
    """ Returns the value packed as a list/iterable. """
    if not isinstance(value, (list, tuple)):
        return [ value ]
    else:
        return value

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "fs",
    "ProgressBar",
    "ProgressBarManager",
    "IndeterminateProgressCycle",
    "log_time",
    "constrain",
    "show_progress",
    "show_indeterminate_progress",
    "iter_progress",
    "as_list",
    "Pipeline"
]
