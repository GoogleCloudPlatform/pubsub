import time
from asyncio import create_task, sleep, wait, Queue, QueueEmpty, Event, Task
from typing import TypeVar, AsyncIterator, AsyncGenerator, List, Callable, Generic, Optional

T = TypeVar("T")

# ValueT is the type returned by valuer, it is used to determine when a batch
# should be truncated.
ValueT = TypeVar("ValueT")


def counter(previous: Optional[int], _: T):
    if previous is None:
        return 1
    return previous + 1


class Batcher(Generic[T]):
    _delay_seconds: float
    _should_truncate: Callable[[ValueT], bool]
    _valuer: Callable[[ValueT, T], ValueT]

    _queue: "Queue[T]"
    _shutdown: Event

    # values controlled by _reset()
    _batch: List[T]
    _batch_value: Optional[ValueT]
    _sleep_until: float

    def __init__(self, delay_seconds: float, should_truncate: Callable[[ValueT], bool],
                 valuer: Callable[[Optional[ValueT], T], ValueT] = counter):
        """
        A batcher creates batches of data from an AsyncIterator.
        :param delay_seconds: The number of seconds after which to truncate the
            batch if it was not yet truncated.
        :param should_truncate: Returns whether the batch with the given value
            should be truncated.  If true, will be truncated without the last
            element added.
        :param valuer: Returns the value of the batch based on the previous
            value and an item to be added.
        """
        self._delay_seconds = delay_seconds
        self._should_truncate = should_truncate
        self._valuer = valuer

    async def batch(self, stream: AsyncIterator[T]) -> AsyncGenerator[List[T], T]:
        assert not hasattr(self, "_queue")
        # Small buffer to allow get_nowait to be called and to prevent always needing to call wait() which is expensive
        # without greatly decoupling the input stream from the output stream
        self._queue = Queue(10)
        self._shutdown = Event()
        enqueue_task = create_task(self._enqueue(stream))
        async for elt in self._make_batches():
            yield elt
        await enqueue_task
        # enqueue task must be already done by the time
        # _make_batches finishes, this is here anyway for correctness.

    async def _enqueue(self, stream: AsyncIterator[T]):
        async for elt in stream:
            await self._queue.put(elt)
        self._shutdown.set()

    async def _make_batches(self) -> AsyncIterator[List[T]]:
        """
        Make a continuous stream of batches by pulling from self._queue
        :return: a continuous stream of batches
        """
        self._reset()
        get_task: Optional[Task] = None
        while not self._shutdown.is_set():
            # get result immediately if available and no task outstanding
            if get_task is None:
                try:
                    result = self._queue.get_nowait()
                    batch_or = self._process_one(result)
                    if batch_or is not None:
                        yield batch_or
                    await sleep(0)  # return control to event loop to allow other actions
                    continue
                except QueueEmpty:
                    pass

            start_iter_time = time.time()
            if self._sleep_until < start_iter_time:  # reset the batch if a timeout occurs
                if self._batch:
                    yield self._batch
                self._reset()

            if get_task is None:
                get_task = create_task(self._queue.get())
            done, _ = await wait([get_task], timeout=(self._sleep_until - start_iter_time))
            if done:
                get_task = None
                result = await done.pop()
                batch_or = self._process_one(result)
                if batch_or is not None:
                    yield batch_or

        async for batch in self._flush(get_task):
            yield batch

    async def _flush(self, get_task: Optional[Task]) -> AsyncIterator[List[T]]:
        """
        Flush the queue until it is empty, ignoring timeouts.
        :param get_task: An already started self._queue.get() coroutine if one exists.
        :return: all the batches created by flushing the queue.
        """
        while not self._queue.empty():
            result: T
            if get_task is None:
                result = await self._queue.get()
            else:
                result = await get_task
                get_task = None
            batch_or = self._process_one(result)
            if batch_or is not None:
                yield batch_or
        if self._batch:
            yield self._batch

    def _process_one(self, to_process: T) -> Optional[List[T]]:
        """
        Process a single value retrieved from the queue
        :param to_process: a value retrieved from the queue
        :return: the batch, if a new one is ready, or None.
        """
        to_return = None
        new_value = self._valuer(self._batch_value, to_process)
        if self._should_truncate(new_value):
            to_return = self._batch
            self._reset()
            new_value = self._valuer(None, to_process)
        # if the single element exceeds the batch limit, still add it to the next batch
        self._batch.append(to_process)
        self._batch_value = new_value
        return to_return

    def _reset(self):
        """
        Reset the batch state after a new batch is generated
        :return: None
        """
        self._batch = []
        self._batch_value = None
        self._sleep_until = time.time() + self._delay_seconds
