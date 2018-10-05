from asyncio import create_task, sleep, wait, FIRST_COMPLETED, Task
from typing import TypeVar, AsyncIterator, AsyncGenerator, List


T = TypeVar("T")


async def batcher(stream: AsyncIterator[T], delay_seconds: float, max_size: int) -> AsyncGenerator[List[T], T]:
    batch = []
    try:
        sleeper: Task = create_task(sleep(delay_seconds))
        yielder: Task = create_task(stream.__anext__())
        while True:
            done, pending = await wait([sleeper, yielder], return_when=FIRST_COMPLETED)
            for one_done in done:
                if one_done is sleeper:
                    if batch:
                        yield batch
                        batch = []
                    sleeper = create_task(sleep(delay_seconds))
                elif one_done is yielder:
                    if len(batch) == max_size:
                        yield batch
                        batch = []
                        sleeper = create_task(sleep(delay_seconds))
                    batch.append(one_done.result())
                    yielder = create_task(stream.__anext__())
    except StopAsyncIteration:
        if batch:
            yield batch
