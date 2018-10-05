from asyncio import get_running_loop, Future
from concurrent.futures import Executor
from typing import AsyncIterator, AsyncGenerator, Callable, Optional

from fast_client.helpers import parallelizer
from fast_client.types import PubsubMessage
from fast_client.processor import Processor


class PoolProcessor(Processor):
    _work: Callable[[PubsubMessage], Optional[str]]
    _executor: Executor

    def __init__(self, work: Callable[[PubsubMessage], Optional[str]], executor: Executor):
        self._work = exception_safe_work(work)
        self._executor = executor

    async def process(self, messages: AsyncIterator[PubsubMessage]) -> AsyncGenerator[str, PubsubMessage]:
        async for maybe_ack_id in parallelizer(messages, self._process_one, 50):
            if maybe_ack_id:
                yield maybe_ack_id

    def _process_one(self, message: PubsubMessage) -> "Future[Optional[str]]":
        to_return = get_running_loop().run_in_executor(self._executor, self._work, message)
        return to_return


def exception_safe_work(work: Callable[[PubsubMessage], Optional[str]]):
    def _wrapped(message: PubsubMessage):
        try:
            return work(message)
        except Exception as e:
            print("user work exception: {}".format(e))
            return None

    return _wrapped
