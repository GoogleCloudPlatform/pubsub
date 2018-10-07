from multiprocessing import Value
from typing import Callable


def default_should_flush(elements: int):
    return elements >= 100


class ElementCounter:
    """
    An MessageCounter is meant to be a cross-process counter.  It does not work when there are multiple threads
    per process.
    """
    _should_flush: Callable[[int], int]

    _global_elements: "Value[int]"
    _process_elements: int = 0

    def __init__(self, should_flush: Callable[[int], int] = default_should_flush):
        self._should_flush = should_flush
        self._global_elements = Value("i", 0)

    def increment(self):
        self._process_elements += 1

        if self._should_flush(self._process_elements):
            self._flush()

    def _flush(self):
        with self._global_elements.get_lock():
            self._global_elements.value += self._process_elements
        self._process_elements = 0

    def read(self) -> int:
        with self._global_elements.get_lock():
            return self._global_elements.value
