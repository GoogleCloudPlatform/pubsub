from inspect import isawaitable
from typing import AsyncIterator, AsyncGenerator, Callable, Optional, Union, Awaitable

from fast_client.types import PubsubMessage
from fast_client.processor import Processor

SyncT = Callable[[PubsubMessage], Optional[str]]
AsyncT = Callable[[PubsubMessage], Awaitable[Optional[str]]]
ActionT = Union[SyncT, AsyncT]


class InlineProcessor(Processor):
    action: ActionT

    def __init__(self, action):
        self.action = action

    async def process(self, messages: AsyncIterator[PubsubMessage]) -> AsyncGenerator[str, PubsubMessage]:
        async for message in messages:
            try:
                result = self.action(message)
                if isawaitable(result):
                    result = await result
                if result is not None:
                    yield result
            except Exception as e:
                print("user work exception: {}".format(e))
