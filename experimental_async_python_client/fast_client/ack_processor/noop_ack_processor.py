from typing import AsyncIterator, AsyncGenerator

from fast_client.ack_processor import AckProcessor


class NoopAckProcessor(AckProcessor):
    async def process_ack(self, ack_ids: AsyncIterator[str]) -> AsyncGenerator[str, str]:
        async for ack_id in ack_ids:
            yield ack_id
