import time
from asyncio import create_task, sleep, wait, FIRST_COMPLETED, Task
from typing import TypeVar, AsyncIterator, AsyncGenerator, List, Callable, Generic, Optional

T = TypeVar("T")


def counter(_: T):
    return 1


class Batcher(Generic[T]):
    _delay_seconds: float
    _max_value: float
    _valuer: Callable[[T], int]

    _stream: AsyncIterator[T]
    _yielder: Task

    # values controlled by _reset()
    _batch: List[T]
    _batch_value: int
    _sleep_until: float

    def __init__(self, delay_seconds: float, max_value: int, valuer: Callable[[T], int] = counter):
        self._delay_seconds = delay_seconds
        self._max_value = max_value
        self._valuer = valuer

    async def batch(self, stream: AsyncIterator[T]) -> AsyncGenerator[List[T], T]:
        assert not hasattr(self, "_stream")
        self._stream = stream
        self._reset_yielder()
        self._reset()
        try:
            while True:
                start_iter_time = time.time()  # reset the time if a timeout occurs
                if self._sleep_until < start_iter_time:
                    if self._batch:
                        yield self._batch
                    self._reset()

                done, _ = await wait([self._yielder], timeout=(self._sleep_until - start_iter_time))
                if done:
                    result = await done.pop()
                    batch_or = self._process_one(result)
                    if batch_or is not None:
                        yield batch_or
                    self._reset_yielder()

        except StopAsyncIteration:
            if self._batch:
                yield self._batch

    def _reset_yielder(self):
        self._yielder = create_task(self._stream.__anext__())

    def _process_one(self, to_process: T) -> Optional[List[T]]:
        to_return = None
        result_value = self._valuer(to_process)
        new_value = self._batch_value + result_value
        if new_value >= self._max_value:
            to_return = self._batch
            self._reset()
        # if the single element exceeds the batch limit, still add it to the next batch
        self._batch.append(to_process)
        self._batch_value += result_value
        return to_return

    def _reset(self):
        self._batch = []
        self._batch_value = 0
        self._sleep_until = time.time() + self._delay_seconds
