from abc import ABC, abstractmethod
from asyncio import Event
from typing import AsyncIterator

from fast_client.types import PubsubMessage


class Reader(ABC):
    @abstractmethod
    async def read(self, shutdown: Event) -> AsyncIterator[PubsubMessage]:
        """
        read should yield a continuous stream of PubsubMessages.  It should eventually return once 'shutdown' is set.
        """
        if False:
            yield