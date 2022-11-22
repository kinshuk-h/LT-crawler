"""

progress
~~~~~~~~

This module defines elements for representing progress on a terminal, as progress bars or
cycles. The module provides implementation of a progress bar, a progress cycle (for
indeterminate progress), and a manager for synchronizing rendering of multiple progress
bars for representing progress across multiple threads or processes.

"""

import threading
import multiprocessing.managers

from math import floor

class IndeterminateProgressCycle:
    """ Defines a single character progress cycle for indeterminate progress. """

    STATES = ( '⢿', '⣻', '⣽', '⣾', '⣷', '⣯', '⣟', '⡿' )

    def __init__(self) -> None:
        self.position = 0

    def advance(self):
        """ Adds progress to the bar, moving the representation to the next state. """
        self.position += 1

    def __str__(self) -> str:
        """ Converts the cycle to a string representation, ready for rendering. """
        return self.STATES[self.position % len(self.STATES)]

class ProgressBar:
    """ Defines a progress bar with fractional increments. """

    STATES = ( ' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█' )

    def __init__(self, limit = 100, size = 30):
        """ Initializes a new ProgressBar.

        Args:
            limit (int, optional): The maximum numeric progress for the bar. Defaults to 100.
            size (int, optional): The size of the bar, when rendered. Defaults to 60.
        """
        self.width, self.length = len(str(limit)), size
        self._position, self.limit = 0, limit

    def __normalize(self):
        """ Normalizes the values of position and completed state markers. """
        self._position =  max(0, min(self._position, self.limit))

    def reset(self, limit = None):
        """ Resets the bar position, clearing all progress, and optionally resets the bar limit.

        Args:
            limit (int, optional): The new maximum progress for the bar. Defaults to None.
        """
        if limit:
            self.width, self.limit = len(str(limit)), limit
        self._position = 0

    def set(self, position):
        """ Sets the progress of the bar.

        Args:
            position (int): The value of progress (clamped to the range [0, limit]).
        """
        self._position = position
        self.__normalize()

    def advance(self, increment = 1):
        """ Adds progress to the bar.

        Args:
            increment (int, optional): The amount of progress to add. Defaults to 1.
        """
        self._position += increment
        self.__normalize()

    @property
    def completed(self):
        """ Determines whether the bar has reached the limit (progress complete) """
        self.__normalize()
        return self._position == self.limit

    def count(self):
        """ Returns a string describing the numeric progress of the bar. """
        return f"({self._position:{self.width}}/{self.limit})"

    def __str__(self):
        """ Converts the bar to a string representation, ready for rendering. """
        fill_length          = floor(self.length * 100 * (self._position/self.limit))
        fill_length_fraction = fill_length % 100
        fill_length          = fill_length // 100
        progress     = fill_length * self.STATES[-1]
        sub_progress = self.STATES[floor(fill_length_fraction/12.5)] if fill_length < self.length else ''
        left         = (self.length-1-fill_length) * self.STATES[0]
        return f"│{progress}{sub_progress}{left}│"

class ProgressBarManager:
    """ Manager for synchonizing multiple progress bars spawned across multiple threads/processes. """

    def __init__(self, size, process_manager: multiprocessing.managers.BaseManager = None) -> None:
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
        """ Add a new bar to the manager, based on given limit data.

        Args:
            limit (int): The limit for the bar to add.
            render (bool, optional): If true, renders the current set of bars. Defaults to False.
            prefix (str, optional): The prefix to render before the newly
                added bar, used only when `render=True`. Defaults to "".
            suffix (str, optional): The suffix to render after the newly
                added bar, used only when `render=True`. Defaults to "".

        Returns:
            int: Index/reference to the added bar, to be used with `update()`.
        """
        with self.lock:
            self.pbars.append( ProgressBar(size=self.size, limit=limit) )
            self.pbar_indices.append(len(self.pbars)-1)
            if render:
                self.last_updated = None
                self.print(prefix, suffix)
            self.active_bar_count += 1
            return self.pbar_indices[-1]

    def update(self, index, increment=1, prefix="", suffix=""):
        """ Utility function for advancing a bar, followed by rending the current set of bars.
            This function is thread-safe, unlike individual `advance()` and `print()` operations.

        Args:
            index (int): Index of the bar to advance, based on a value returned by `add()`.
            increment (int, optional): The value to advance the bar with. Defaults to 1.
            prefix (str, optional): Prefix to render before the bar. Defaults to "".
            suffix (str, optional): Suffix to render after the bar. Defaults to "".
        """
        with self.lock:
            self.advance(index, increment)
            self.print(prefix, suffix)

    def advance(self, index, increment):
        """ Adds progress to one of the current set of bars.

            This operation is not thread-safe. In case a thread-safe version
            is required (usually for `advance()` followed by `print()`), use `update()`.

        Args:
            index (int): Index to the bar to advance, as returned by `add()`.
            increment (int): The progress to add. Defaults to 1.
        """
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
        """ Renders the current set of bars to the console (STDOUT).
            For rendering, ensure that the terminal supports ANSI escape sequences.

            This operation is not thread-safe. In case a thread-safe version
            is required (usually for `advance()` followed by `print()`), use `update()`.

        Args:
            prefix (str, optional): Prefix string to render before the last updated bar. Defaults to "".
                If no bar was recently advanced, then the prefix is rendered before all the bars.
            suffix (str, optional): Suffix string to render before the last updated bar. Defaults to "".
                If no bar was recently advanced, then the suffix is rendered after all the bars.
        """
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
        print('\x1B[1A\r', end='')
