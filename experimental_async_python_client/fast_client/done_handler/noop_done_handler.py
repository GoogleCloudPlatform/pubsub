from typing import AsyncIterator, Coroutine, Awaitable

from fast_client.done_handler import DoneHandler


class NoopDoneHandler(DoneHandler):
    async def done(self, completed_ids: AsyncIterator[str]) -> Awaitable[None]:
        async for _ in completed_ids:
            pass
