from abc import ABC, abstractmethod
from typing import AsyncGenerator, AsyncIterator

from fast_client.types import PubsubMessage, UserPubsubMessage


class Writer(ABC):
    @abstractmethod
    def write(self, to_write: AsyncIterator[UserPubsubMessage]) -> AsyncGenerator[PubsubMessage, UserPubsubMessage]:
        pass
