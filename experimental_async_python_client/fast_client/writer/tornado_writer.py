from typing import AsyncIterator, AsyncGenerator, List

from tornado.httpclient import AsyncHTTPClient

from fast_client.helpers import batcher
from fast_client.types import UserPubsubMessage, PubsubMessage, TopicInfo
from fast_client.writer import Writer


class TornadoWriter(Writer):
    _topic_info: TopicInfo
    _client: AsyncHTTPClient

    def __init__(self, topic_info: TopicInfo, client: AsyncHTTPClient):
        self._topic_info = topic_info
        self._client = client

    def write(self, to_write: AsyncIterator[UserPubsubMessage]) -> AsyncGenerator[PubsubMessage, UserPubsubMessage]:
        batch_gen = batcher(to_write, 5, 1000)
        safe_batch_gen = self._batch_divider(batch_gen, 8 * 10**9)  # 8 mb limit on the batch

    @staticmethod
    def _batch_divider(
            batches: AsyncIterator[List[UserPubsubMessage]], byte_limit: int) -> AsyncGenerator[
        List[UserPubsubMessage], List[UserPubsubMessage]]:
        async for batch in batches:
            byte_count: int = 0
            new_batch: List[UserPubsubMessage] = []
            for message in batch:
                new_bytes = byte_count + len(message.data)
                if new_bytes > byte_limit:
                    if new_batch:
                        yield new_batch
                        new_batch = []
                        byte_count = 0
                new_batch.append(message)
                byte_count += len(message.data)
            if new_batch:
                yield new_batch

