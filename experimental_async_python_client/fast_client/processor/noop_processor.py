from typing import AsyncIterator, AsyncGenerator

from fast_client.types import PubsubMessage
from fast_client.processor import Processor


class NoopProcessor(Processor):
    async def process(self, messages: AsyncIterator[PubsubMessage]) -> AsyncGenerator[str, PubsubMessage]:
        async for message in messages:
            yield message.ack()
