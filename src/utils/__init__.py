import os
import hashlib
import threading
import collections

from .progress import ProgressBar, IndeterminateProgressCycle, ProgressBarManager

def chunk_reader(file_descriptor, chunk_size = 4096):
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

class FileIndexStore:
    """ Utility class to maintain a record of existing files. """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.data = collections.defaultdict(lambda: collections.defaultdict(dict))

    def load(self, filepath, group, index_info=None, callback=None):
        """ Loads a filepath into the index store, under the specified group.

        Args:
            filepath (str): The path to the file to load.
            group (str): The group to store the file data in.
            index_info (dict, optional): Optional dict containing information about
                file size and hashes. Defaults to None.
            callback ((*args) -> None, optional): Optional callback to invoke upon completion. Defaults to None.

        Returns:
            dict: Information about the file's size and hash.
        """
        index_info = index_info or {}
        if not index_info.get('hash', None):
            index_info['hash']    = file_digest(filepath)
        if not index_info.get('minhash', None):
            index_info['minhash'] = file_digest(filepath, primary_chunk_only=True)
        if not index_info.get('size', None):
            index_info['size']    = os.stat(filepath).st_size

        with self.lock:
            self.data[group]['size'   ][index_info['size'   ]] = filepath
            self.data[group]['minhash'][index_info['minhash']] = filepath
            self.data[group]['hash'   ][index_info['hash'   ]] = filepath

        if callback: callback(filepath, index_info)

        return index_info

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
        info = {
            'size': os.stat(filepath).st_size if filepath else 0,
            'minhash': None, 'hash': None
        }

        status = False
        if filepath is not None:
            if info['size'] in self.data[group]['size']:
                info['minhash'] = file_digest(filepath, primary_chunk_only=True)
                if info['minhash'] in self.data[group]['minhash']:
                    info['hash'] = file_digest(filepath)
                    status = info['hash'] in self.data[group]['hash']
                    info['match'] = self.data[group]['hash'].get(info['hash'], None)

        if return_info:
            return status, info
        else:
            return status

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [
    "ProgressBar",
    "ProgressBarManager",
    "IndeterminateProgressCycle",
    "FileIndexStore",
]
