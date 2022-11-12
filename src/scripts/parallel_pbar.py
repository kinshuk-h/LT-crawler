import os
import re
import sys
import time
import tempfile
import threading
import multiprocessing
import concurrent.futures

from math import floor
from urllib.parse import urlparse

import requests

__SIZE_UNITS__    = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")
__SIZE_UNITS_SI__ = ("", "k" , "M" , "G" , "T" , "P" , "E" , "Z" , "Y" )
def unitize_size(size_in_bytes, use_si = False, ratio = 0.7):
    unit_index = 0; factor = 1000 if use_si else 1024
    while size_in_bytes > factor * ratio: size_in_bytes /= factor; unit_index += 1
    unit = __SIZE_UNITS_SI__[unit_index] if use_si else __SIZE_UNITS__[unit_index]
    return f"{size_in_bytes:.3f} {unit}B"

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

def task(args):
    global_dict, url = args
    manager = global_dict['manager']

    response = requests.get(url, stream=True)
    response.raise_for_status()
    if "image" not in response.headers['Content-Type']:
      raise ValueError("get_image(): invalid URL, referring to non-image content")
    if "Content-Disposition" in response.headers.keys():
      fname = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
    else:
      fname = os.path.basename(urlparse(response.url).path)
    path = fname
    total_size = int(response.headers['Content-Length'])
    total_size_str = unitize_size(total_size)
    index = manager.add(limit=total_size)
    w = len(total_size_str)
    with open(os.path.join(tempfile.gettempdir(), path), "wb+") as file:
        current_size = 0
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)
            current_size += len(chunk)
            manager.update(index, increment=len(chunk), prefix=f"{fname[:50]:50}", suffix=f"{unitize_size(current_size):>11} / {total_size_str:>11}")
    return path

def main():
    # with multiprocessing.Manager() as process_manager:
    #     global_dict = process_manager.dict()
    #     global_dict['manager'] = ProgressBarManager(10, process_manager)
    #     with concurrent.futures.ProcessPoolExecutor() as executor:
    #         print([
    #             *executor.map(task, [ (global_dict, limit) for limit in [ 100, 200, 50 ] ])
    #         ])

    global_dict = {
        'manager': ProgressBarManager(10)
    }
    # limits = [ 100, 50, 90, 70, 80, 90, 60, 10, 100 ]
    test_images = [
        "https://www.humanesociety.org/sites/default/files/styles/1240x698/public/2020-07/kitten-510651.jpg?h=f54c7448&itok=ZhplzyJ9",
        "https://hips.hearstapps.com/hmg-prod.s3.amazonaws.com/images/close-up-of-cat-wearing-sunglasses-while-sitting-royalty-free-image-1571755145.jpg",
        "https://pbs.twimg.com/media/EAmr-PAWsAEoiWR.jpg",
        "https://i.ytimg.com/vi/QH2-TGUlwu4/sddefault.jpg",
        "https://images.unsplash.com/photo-1587402092301-725e37c70fd8?ixlib=rb-1.2.1&ixid=MnwxMjA3fDB8MHxzZWFyY2h8MXx8cHVwcHklMjBkb2d8ZW58MHx8MHx8&w=1000&q=80",
        "https://techcrunch.com/wp-content/uploads/2020/01/MarsCat-6.jpg"
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        [
            *executor.map(task, [ (global_dict, url) for url in test_images ])
        ]

    # manager.add(limit=100)
    # manager.add(limit=200)
    # manager.add(limit=100)

    # manager.print(prefix=f"{'':9}")

    # iteration_number = 0

    # # time.sleep(5)
    # manager.advance(0, 1)
    # manager.print(prefix=f"tick={iteration_number:4}")

    # time.sleep(10)
    # for _ in range(max(bar.limit for bar in (manager.pbars))):
    #     time.sleep(0.05)
    #     manager.advance(0, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1
    #     time.sleep(0.05)
    #     manager.advance(1, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1
    #     time.sleep(0.05)
    #     manager.advance(2, 1)
    #     manager.print(prefix=f"tick={iteration_number:4}")
    #     iteration_number += 1

    print()

    bar = ProgressBar(limit=100, size=10)
    for _ in range(bar.limit + 10):
        bar.advance()
        print('\r', f"{' ':50} {bar}", end='')
    print()

if __name__ == "__main__":
    main()