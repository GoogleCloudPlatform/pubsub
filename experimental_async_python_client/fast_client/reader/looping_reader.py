import datetime
from asyncio import Event
from typing import AsyncIterator

from fast_client.core import PubsubMessage
from fast_client.reader import Reader

message = PubsubMessage("", {}, "", datetime.datetime.now(), "")


class LoopingReader(Reader):
    async def read(self, shutdown: Event) -> AsyncIterator[PubsubMessage]:
        while not shutdown.is_set():
            yield message
