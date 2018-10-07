from asyncio import Event, iscoroutinefunction
from functools import partial
from typing import AsyncIterator, Awaitable

from fast_client.message_generator import MessageGenerator
from fast_client.core import UserPubsubMessage
from fast_client.core.callbacks import PublisherActionT, PublisherAsyncT, PublisherSyncT


async def sync_awaitable(action: PublisherSyncT) -> Awaitable[UserPubsubMessage]:
    return action()


class InlineMessageGenerator(MessageGenerator):
    _action: PublisherAsyncT

    def __init__(self, action: PublisherActionT):
        if iscoroutinefunction(action):
            self._action = action
        else:
            self._action = partial(sync_awaitable, action)

    async def generate(self, shutdown: Event) -> AsyncIterator[UserPubsubMessage]:
        while not shutdown.is_set():
            try:
                yield await self._action()
            except Exception as e:
                print("user work exception: {}".format(e))
