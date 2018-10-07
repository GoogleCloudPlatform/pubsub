from functools import partial
from inspect import iscoroutinefunction
from typing import AsyncIterator, AsyncGenerator, Callable, Optional, Union, Awaitable

from fast_client.core import PubsubMessage
from fast_client.processor import Processor
from fast_client.core.callbacks import SubscriberActionT, SubscriberSyncT, SubscriberAsyncT


async def sync_awaitable(action: SubscriberSyncT, message: PubsubMessage) -> Awaitable[Optional[str]]:
    return action(message)


class InlineProcessor(Processor):
    _action: SubscriberAsyncT

    def __init__(self, action: SubscriberActionT):
        if iscoroutinefunction(action):
            self._action = action
        else:
            self._action = partial(sync_awaitable, action)

    async def process(self, messages: AsyncIterator[PubsubMessage]) -> AsyncGenerator[str, PubsubMessage]:
        async for message in messages:
            try:
                ackId = await self._action(message)
                if ackId is not None:
                    yield ackId
            except Exception as e:
                print("user work exception: {}".format(e))
