"""
Helper class for measuring the execution time of a block of code.
"""

import time
from typing import Optional


class Timer:
    """
    A context manager for measuring the execution time of a block of code.

    Usage:
    ```
    with Timer() as timer:
        # code to be timed
        print(f"Current duration: {timer.duration} seconds") # Access duration within the context
    print(f"Execution time: {timer.duration} seconds")
    ```
    """

    __slots__ = ("_start", "_end")

    def __init__(self) -> None:
        self._start: Optional[float] = None
        self._end: Optional[float] = None

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        self._end = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # We know _start was set in __enter__, so no need to check again
        self._end = time.perf_counter()

    @property
    def duration(self) -> Optional[float]:
        """
        Returns the duration of the timer in seconds.

        If the timer has not been started, returns None.
        If the timer is still running, returns the elapsed time since it was started.
        If the timer has been stopped, returns the total elapsed time.
        """
        if self._start is None:
            # timer was never entered
            return None

        if self._end is not None:
            # timer has been stopped
            return self._end - self._start
        # timer is still running
        return time.perf_counter() - self._start
