from abc import ABC, abstractmethod
from typing import AsyncGenerator, AsyncIterator

from fast_client.core import UserPubsubMessage


class Writer(ABC):
    @abstractmethod
    async def write(self, to_write: AsyncIterator[UserPubsubMessage]) -> AsyncGenerator[str, UserPubsubMessage]:
        pass
