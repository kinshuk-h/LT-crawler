import os
import hashlib
import threading
import collections

from math import floor

class ProgressBar:
    """ Defines a progress bar with fractional increments. """
    states = ( ' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█' )
    def __init__(self, limit = 100, size = 60):
        self.width = len(str(limit))
        self.length = size
        self.limit = limit
        self.reset()
    def reset(self, limit = None):
        if limit:
            self.width = len(str(limit))
            self.limit = limit
        self.position = 0
        self._completed = False
    def set(self, position):
        self.position = position
        if self.position >= self.limit: self._completed = True
    def advance(self, increment = 1):
        if(self.position < self.limit): self.position += increment
        else: self._completed = True
    @property
    def completed(self):
        return self._completed
    def count(self): return f"({self.position:{self.width}}/{self.limit})"
    def __str__(self):
        fill_length          = floor(self.length * 100 * (self.position/self.limit))
        fill_length_fraction = fill_length % 100
        fill_length          = fill_length // 100
        progress     = fill_length * self.states[-1]
        sub_progress = self.states[floor(fill_length_fraction/12.5)] if fill_length < self.length else ''
        left         = (self.length-1-fill_length) * self.states[0]
        return f"│{progress}{sub_progress}{left}│"

class ProgressBarManager:
    """ Manager for synchonizing multiple progress bars spawned across multiple threads. """
    def __init__(self, size, process_manager = None) -> None:
        if process_manager is not None:
            self.lock = process_manager.Lock()
        else:
            self.lock = threading.Lock()
        self.pbars = []
        self.pbar_indices = []
        self.size = size
        self.last_updated = None
        self.last_completed = None
        self.active_bar_count = 0
        self.printed_once = False
        self.cursor_position = 0

    def add(self, limit, render=False, prefix="", suffix=""):
        with self.lock:
            self.pbars.append(
                ProgressBar(size=self.size, limit=limit)
            )
            self.pbar_indices.append(len(self.pbars)-1)
            if render:
                self.last_updated = None
                self.print(prefix, suffix)
            self.active_bar_count += 1
            return self.pbar_indices[-1]

    def update(self, index, increment=1, prefix="", suffix=""):
        with self.lock:
            self.advance(index, increment)
            self.print(prefix, suffix)

    def advance(self, index, increment):
        previously_completed = self.pbars[index].completed
        self.pbars[index].advance(increment)
        if self.pbars[index].completed and not previously_completed:
            self.pbar_indices.remove(index)
            for _index in self.pbar_indices:
                if not self.pbars[_index].completed:
                    self.pbar_indices.insert(max(_index-1, 0), index)
                    break
            else:
                self.pbar_indices.append(index)
            self.last_completed = index
        self.last_updated = index

    def print(self, prefix = "", suffix = ""):
        # if self.last_updated is not None:
            # if not self.printed_once:
            #     last_updated = self.last_updated
            #     self.last_updated = None
            #     if self.last_bar_count > 0:
            #         new_bar_count, self.active_bar_count = self.active_bar_count, self.last_bar_count
            #     self.print()
            #     if self.last_bar_count > 0:
            #         self.last_bar_count, self.active_bar_count = 0, new_bar_count
            #     self.last_updated = last_updated
        if self.cursor_position > 0:
            print(f"\r\x1B[{self.cursor_position}A")
            self.cursor_position = 0
        # else:
        #     self.printed_once = True
        for index in self.pbar_indices:
            if self.pbars[index].completed:
                if self.last_completed is not None and index == self.last_completed:
                    self.last_completed = None
                    self.active_bar_count -= 1
                    print('\r', prefix, self.pbars[index], suffix)
                else:
                    continue
            elif self.last_updated is not None and self.last_updated != index:
                print()
                self.cursor_position += 1
            else:
                print('\r', prefix, self.pbars[index], suffix)
                self.cursor_position += 1
        # print("\r", self.cursor_position, end='')
        # time.sleep(2)
        print('\x1B[1A\r', end='')
        # time.sleep(2)

__version__ = "1.0.0"
__author__  = "Kinshuk Vasisht"
__all__     = [ "ProgressBar", "ProgressBarManager" ]

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
    """ Returns a file digest describing the contents of the file.

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
        self.data = collections.defaultdict(collections.defaultdict(dict))

    def load(self, filepath, group, index_info=None):
        index_info = index_info or {}
        if not index_info.get('hash', None):
            index_info['hash']    = file_digest(filepath)
        if not index_info.get('minhash', None):
            index_info['minhash'] = file_digest(filepath, primary_chunk_only=True)
        if not index_info.get('size', None):
            index_info['size']    = os.stat().st_size

        with self.lock:
            self.data[group]['size'   ][index_info['size'   ]] = filepath
            self.data[group]['minhash'][index_info['minhash']] = filepath
            self.data[group]['hash'   ][index_info['hash'   ]] = filepath

    def has(self, filepath, group, return_info=False):
        info = {
            'size': os.stat(filepath).st_size,
            'minhash': None, 'hash': None
        }

        status = False
        if info['size'] in self.data[group]['size']:
            info['minhash'] = file_digest(filepath, primary_chunk_only=True)
            if info['minhash'] in self.data[group]['minhash']:
                info['hash'] = file_digest(filepath)
                status = info['hash'] in self.data[group]['hash']

        if return_info:
            return status, info
        else:
            return status
