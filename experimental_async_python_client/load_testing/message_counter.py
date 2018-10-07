from multiprocessing import Value
from typing import Callable, Tuple

from fast_client.core import PubsubMessage


def default_should_flush(message_count: int, byte_count: int):
    return message_count >= 100


class MessageCounter:
    """
    A MessageCounter is meant to be a cross-process counter.  It does not work when there are multiple threads
    per process.
    """
    _should_flush: Callable[[int, int], int]

    _global_messages: "Value[int]"
    _global_bytes: "Value[int]"
    _process_messages: int = 0
    _process_bytes: int = 0

    def __init__(self, should_flush: Callable[[int, int], bool] = default_should_flush):
        self._should_flush = should_flush
        self._global_messages = Value("i", 0)
        self._global_bytes = Value("i", 0)

    def increment(self, message: PubsubMessage):
        self._process_messages += 1
        self._process_bytes += len(message.data)

        if self._should_flush(self._process_messages, self._process_bytes):
            self._flush()

    def _flush(self):
        with self._global_messages.get_lock():
            self._global_messages.value += self._process_messages
        with self._global_bytes.get_lock():
            self._global_bytes.value += self._process_bytes
        self._process_messages = 0
        self._process_bytes = 0

    def read(self) -> Tuple[int, int]:
        global_messages = -1
        global_bytes = -1

        with self._global_messages.get_lock():
            global_messages = self._global_messages.value
        with self._global_bytes.get_lock():
            global_bytes = self._global_bytes.value

        return global_messages, global_bytes
