from abc import ABC, abstractmethod
from typing import AsyncIterator, AsyncGenerator

from fast_client.core import PubsubMessage


class Processor(ABC):
    @abstractmethod
    async def process(self, messages: AsyncIterator[PubsubMessage]) -> AsyncGenerator[str, PubsubMessage]:
        """
        process should yield a continuous stream of ackIds to ack.  Not all messages need to be acked, and those that
        are not will eventually be redelivered.

        :param messages: An iterator which yields a continuous stream of PubsubMessages
        """
        if False:
            yield
