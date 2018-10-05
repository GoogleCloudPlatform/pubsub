from abc import ABC, abstractmethod
from typing import AsyncIterator, Coroutine


class AckDoneHandler(ABC):
    @abstractmethod
    async def ack_done(self, completed_ack_ids: AsyncIterator[str]) -> Coroutine:
        """
        ack_done should never yield.  It does not have to, but may do anything in response to ack completions.

        :param completed_ack_ids: An iterator which yields a continuous stream of completed ackIds
        """
        pass
