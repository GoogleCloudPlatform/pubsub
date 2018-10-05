from asyncio import Task, create_task, wait, FIRST_COMPLETED, Future
from itertools import chain
from typing import TypeVar, AsyncIterator, Callable, AsyncGenerator, Set

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


async def parallelizer(inputs: AsyncIterator[InputT],
                       action: Callable[[InputT], "Future[OutputT]"],
                       max_outstanding: int) -> AsyncGenerator[OutputT, InputT]:
    action_futures: Set["Future[OutputT]"] = set()
    try:
        input_task: Task = create_task(inputs.__anext__())
        while True:
            if len(action_futures) < max_outstanding:
                done, pending = await wait(chain(action_futures, [input_task]), return_when=FIRST_COMPLETED)
            else:
                done, pending = await wait(action_futures, return_when=FIRST_COMPLETED)
            for one_done in done:
                if one_done is input_task:
                    action_future = action(one_done.result())
                    action_futures.add(action_future)
                    input_task = create_task(inputs.__anext__())
                else:  # one_done is in action_futures
                    action_futures.remove(one_done)
                    yield one_done.result()
    except StopAsyncIteration:
        while action_futures:
            done, pending = await wait(action_futures, return_when=FIRST_COMPLETED)
            for one_done in done:
                action_futures.remove(one_done)
                yield one_done.result()
