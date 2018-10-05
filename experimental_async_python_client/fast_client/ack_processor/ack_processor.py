from abc import ABC, abstractmethod
from typing import AsyncIterator, AsyncGenerator


class AckProcessor(ABC):
    @abstractmethod
    async def process_ack(self, ack_ids: AsyncIterator[str]) -> AsyncGenerator[str, str]:
        """
        process_ack should yield a continuous stream of completed ackIds.  All ackIds yielded from `ack_ids` should
        eventually be returned.

        :param ack_ids: An iterator which yields a continuous stream of ackIds
        """
        if False:
            yield