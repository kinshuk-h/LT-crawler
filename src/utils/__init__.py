import threading

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