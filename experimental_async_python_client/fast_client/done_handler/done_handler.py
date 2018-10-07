from abc import ABC, abstractmethod
from typing import AsyncIterator, Awaitable


class DoneHandler(ABC):
    @abstractmethod
    async def done(self, completed_ids: AsyncIterator[str]) -> Awaitable[None]:
        """
        done should never yield.  It does not have to, but may do anything in response to request completions.

        :param completed_ids: An iterator which yields a continuous stream of identifiers for completed requests
        """
        pass
