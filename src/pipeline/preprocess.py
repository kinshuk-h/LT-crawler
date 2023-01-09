"""

    preprocess
    ~~~~~~~~~~

    This module provides the preprocessing functions and associated utilities for the pipeline.
    These functions perform the following tasks:

    - Build indexes over judgments, storing case numbers and URLs of existing judgments for efficient retrieval
    - Build indexes over documents, storing file hashes of downloaded and processed files.

    Author : Kinshuk Vasisht
    Version: 1.0.0
    Dated  : 2022-01-04

"""

import os
import sys
import glob
import typing
import hashlib
import traceback
import threading
import collections
import concurrent.futures
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

from .. import utils
from . import logger

# ==== Helper functions

def remove_query_param(url, key):
    """ Removes a key from the query segment of a URL.

    Args:
        url (str): The URL to remove from.
        key (str): The key to remove.

    Returns:
        str: The new URL with the reduced query segment.
    """
    url_res = urlparse(url)
    query = parse_qs(url_res.query, keep_blank_values=True)
    query.pop(key, None)
    url_res = url_res._replace(query=urlencode(query, True))
    return urlunparse(url_res)

def chunk_reader(file_descriptor: typing.IO, chunk_size = 4096):
    """ Creates a generator over a file descriptor to read a file in chunks.

    Args:
        file_descriptor (IO): File descriptor, or any object supporting read.
        chunk_size (int, optional): Chunk size to yield at a time. Defaults to 4KiB (4096B).

    Yields:
        bytes: The binary chunk read from the file.
    """
    while True:
        chunk = file_descriptor.read(chunk_size)
        if not chunk:
            return
        yield chunk

def file_digest(filepath, primary_chunk_only = False):
    """ Returns a file digest describing the contents of a file.

    Args:
        filepath (str): Path to the file to create a digest for.
        primary_chunk_only (bool, optional): If true, returns a digest of only the first
            1024 bytes from the file. Defaults to False.

    Returns:
        bytes: The SHA1 digest of the file contents (complete or partial).
    """
    hash_object = hashlib.sha1()
    with open(filepath, 'rb') as file:
        if primary_chunk_only:
            hash_object.update(file.read(1024))
        else:
            for chunk in chunk_reader(file):
                hash_object.update(chunk)
    return hash_object.digest()

# ==== Utility classes for data indexing

class FileIndexStore:
    """ Utility class to maintain a record of existing files by indexing using file sizes and hashes. """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.data = collections.defaultdict(lambda: collections.defaultdict(dict))

    @utils.log_time(logger)
    def load_directory(self, directory, file_glob="*.*", metadata_map=None, callback=None):
        """ Loads multiple files from a given directory into the index store.
            This function may utilize multiple threads to perform I/O concurrently.

        Args:
            directory (str): The path to the directory to load from. The basename is used as the group.
            file_glob (str, optional): Optional glob pattern to load specific files. Defaults to *.*
            metadata_map ((*args) -> any, optional): Callback to generate metadata for file entries.
            callback ((*args) -> None, optional): Optional callback to invoke upon
                completion of every load operation. Defaults to None.

        Returns:
           list[dict] : List of file information dictionaries.
        """
        group = os.path.basename(directory)
        files = glob.glob(file_glob, root_dir=directory)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            file_index_infos = [ *executor.map(
                self.get_indexing_info, (os.path.join(directory, file) for file in files),
            ) ]
        for file, index_info in zip(files, file_index_infos):
            meta = metadata_map(file) if metadata_map else file
            self.load(os.path.join(directory, file), group, index_info, meta, callback)

    def get_indexing_info(self, filepath):
        """ Computes information useful for indexing, such as byte size and hashes.

        Args:
            filepath (string): Path to the file to process.
        """
        index_info = {}
        if not index_info.get('hash', None):
            index_info['hash']    = file_digest(filepath)
        if not index_info.get('minhash', None):
            index_info['minhash'] = file_digest(filepath, primary_chunk_only=True)
        if not index_info.get('size', None):
            index_info['size']    = os.stat(filepath).st_size
        return index_info

    def load(self, filepath, group, index_info=None, meta=None, callback=None):
        """ Loads a filepath into the index store, under the specified group.

        Args:
            filepath (str): The path to the file to load.
            group (str): The group to store the file data in.
            index_info (dict, optional): Optional dict containing information about
                file size and hashes. Defaults to None.
            meta (any, optional): Additional metadata to store with the index entry.
            callback ((*args) -> None, optional): Optional callback to invoke upon completion. Defaults to None.

        Returns:
            dict: Information about the file's size and hash.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"No such file: {filepath}")

        index_info = index_info or self.get_indexing_info(filepath)
        if not index_info.get('hash', None):
            index_info['hash']    = file_digest(filepath)
        if not index_info.get('minhash', None):
            index_info['minhash'] = file_digest(filepath, primary_chunk_only=True)
        if not index_info.get('size', None):
            index_info['size']    = os.stat(filepath).st_size

        with self.lock:
            if index_info['size'   ] not in self.data[group]['size'   ]:
                self.data[group]['size'   ][index_info['size'   ]] = filepath
            if index_info['minhash'] not in self.data[group]['minhash']:
                self.data[group]['minhash'][index_info['minhash']] = filepath
            if index_info['hash'   ] not in self.data[group]['hash'   ]:
                self.data[group]['hash'   ][index_info['hash'   ]] = filepath
                self.data[group]['data'][filepath] = meta

        if callback: callback(filepath, index_info)

        return index_info

    def get(self, filepath, group, return_info=False):
        """ Returns the entry associated for a file in the index store.

        Args:
            filepath (str): Path to the file to check.
            group (str): Group to search the file in.
            return_info (bool, optional): If true, returns computed hash and size
            information, useful for loading the entry into the index store. Defaults to False.

        Returns:
            tuple(any, dict) | any: Associated metadata for a given entry,
                and an optional dictionary of file information.
        """
        info = { 'size': 0, 'minhash': None, 'hash': None }

        status = False
        if filepath is not None:
            if os.path.exists(filepath):
                info['size'] = os.stat(filepath).st_size
                if info['size'] in self.data[group]['size']:
                    info['minhash'] = file_digest(filepath, primary_chunk_only=True)
                    if info['minhash'] in self.data[group]['minhash']:
                        info['hash'] = file_digest(filepath)
                        status = info['hash'] in self.data[group]['hash']
                        info['match'] = self.data[group]['hash'].get(info['hash'], None)

        meta = self.data[group]['data'][info['match']] if status else None
        if return_info:
            return meta, info
        else:
            return meta

    def has(self, filepath, group, return_info=False):
        """ Checks if a given file is present in the index store.

        Args:
            filepath (str): Path to the file to check.
            group (str): Group to search the file in.
            return_info (bool, optional): If true, returns computed hash and size
            information, useful for loading the entry into the index store. Defaults to False.

        Returns:
            [bool, (bool, dict)]: A boolean denoting file presence,
                and an optional dictionary of file information.
        """

        meta, info = self.get(filepath, group, return_info=True)
        status = meta is not None
        if return_info:
            return status, info
        else:
            return status

class JudgmentIndexStore:
    """ Utility class to maintain a record of existing judgments by indexing using case numbers and URLs. """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.data = collections.defaultdict(lambda: { 'urls': {}, 'case': {}, 'data': [] })

    def load(self, judgment: dict, group: str, metadata):
        """ Loads a new entry into the judgment index store.

        Args:
            judgment (dict): Judgment object to load.
            group (str): Group for the judgment.
            metadata (any): Additional information to be stored alongside the judgment.
        """
        urls     = utils.as_list(judgment['document_href'])
        case_nos = utils.as_list(judgment['case_number'  ])

        for url, case_no in zip(urls, case_nos):
            url     = remove_query_param(url, 'ID')
            self.data[group]['urls'][url]     = len(self.data[group]['data'])
            self.data[group]['case'][case_no] = len(self.data[group]['data'])
        self.data[group]['data'].append({ 'data': judgment, 'meta': metadata })

    def get(self, judgment: dict, group: str, default=None):
        """ Retrieves the associated judgment metadata for a judgment.

        Args:
            judgment (dict): Judgment object to search.
            group (str): Group for the judgment.
            default (any, optional): Default object to return in case nothing was found. Defaults to None.
        Returns:
            tuple(dict|None, any|None): Tuple of None's if no value is found, else the entry and associated metadata.
        """
        if (case_no := judgment.get('case_number', judgment.get('case', None))) is not None:
            if (index := self.data[group]['case'].get(case_no, -1)) != -1:
                return tuple(self.data[group]['data'][index].values())
        elif (url := judgment.get('document_href', judgment.get('url', None))) is not None:
            url = remove_query_param(url, 'ID')
            if (index := self.data[group]['urls'].get(url, -1)) != -1:
                return tuple(self.data[group]['data'][index].values())
        return default, None

    def has(self, judgment: dict, group: str):
        """ Checks if a judgment is present in the index store.
            Equivalent to `self.get(judgment, group)[0] is not None`.

        Args:
            judgment (dict): Judgment object to search.
            group (str): Group for the judgment.

        Returns:
            bool: True if found, otherwise False.
        """
        return self.get(judgment, group)[0] is not None

# === Main pipeline phase implementation

def map_from_index(dict_index: dict):
    """ Mapping function to map a file prefix to associated entry from a dictionary. """
    def map_from_index_impl(file):
        return dict_index.get(os.path.splitext(file)[0], None)
    return map_from_index_impl

def load_indexes(prog, args):
    """ Pre-processing stage: Load file and judgment indexes for detecting duplicates. """
    file_index     = FileIndexStore()
    judgment_index = JudgmentIndexStore()

    print(prog, ": building file & judgment index store ...", sep='')
    for court in args.courts:
        output_dir = os.path.join(args.output_dir, args.document_dir, f"{court} Judgments")
        json_dir   = os.path.join(args.output_dir, "json", f"{court} Judgments")
        json_index = {}
        if os.path.exists(output_dir) and os.path.exists(json_dir):
            # Build judgment index
            try:
                print(f"  : loading information for {court} ... ", sep='', end = '', flush=True)
                for file_name in utils.iter_progress(os.listdir(json_dir)):
                    if not file_name.endswith('.json'): continue
                    data = utils.fs.read_json(os.path.join(json_dir, file_name))
                    for index, judgment in enumerate(data['data']):
                        if judgment.get('document_path', None) is not None:
                            key = os.path.splitext(os.path.basename(judgment['document_path']))[0]
                            meta = {
                                'json': os.path.join(json_dir, file_name),
                                'index': index
                            }
                            judgment_index.load(judgment, court, meta)
                            json_index[key] = meta
                print("\b\bdone")
            except Exception as exc:
                print("\b\berror")
                print(prog, ": error: ", exc, sep='', file=sys.stderr)
                logger.exception("error")
                if args.debug:
                    traceback.print_exc()

            # Build file index
            try:
                print(f"  : loading hashes for {court} ... ", sep='', end = '', flush=True)
                file_index.load_directory(
                    output_dir, "*.pdf", map_from_index(json_index),
                    utils.show_indeterminate_progress()
                )
                print("\b\bdone")
            except Exception as exc:
                print("\b\berror")
                print(prog, ": error: ", exc, sep='', file=sys.stderr)
                logger.exception("error")
                if args.debug:
                    traceback.print_exc()

            for extractor in args.extractors:
                extract_output_dir = os.path.join(output_dir, utils.fs.pathsafe("extracted_" + extractor))
                if os.path.exists(output_dir):
                    try:
                        print(f"  : loading hashes for {extractor} ... ", sep='', end = '', flush=True)
                        file_index.load_directory(
                            extract_output_dir, "*.txt", map_from_index(json_index),
                            utils.show_indeterminate_progress()
                        )
                        print("\b\bdone")
                    except Exception as exc:
                        print("\b\berror")
                        print(prog, ": error: ", exc, sep='', file=sys.stderr)
                        logger.exception("error")
                        if args.debug:
                            traceback.print_exc()
    print()
    return file_index, judgment_index
