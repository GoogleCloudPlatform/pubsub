from abc import ABC, abstractmethod
from asyncio import Event
from typing import AsyncIterator

from fast_client.core import UserPubsubMessage


class MessageGenerator(ABC):
    @abstractmethod
    async def generate(self, shutdown: Event) -> AsyncIterator[UserPubsubMessage]:
        pass
