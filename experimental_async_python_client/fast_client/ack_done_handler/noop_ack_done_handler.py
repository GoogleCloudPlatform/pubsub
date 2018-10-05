from typing import AsyncIterator, Coroutine

from fast_client.ack_done_handler import AckDoneHandler


class NoopAckDoneHandler(AckDoneHandler):
    async def ack_done(self, completed_ack_ids: AsyncIterator[str]) -> Coroutine:
        async for _ in completed_ack_ids:
            pass
