import json
from typing import AsyncIterator, AsyncGenerator, List, Awaitable, Dict

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from fast_client.helpers import Batcher, parallelizer, retrying_requester
from fast_client.core import UserPubsubMessage, TopicInfo, UserPubsubMessageEncoder
from fast_client.writer import Writer


class TornadoWriterOptions:
    write_batch_latency_seconds: float = 5
    write_batch_size_bytes: int = 10**6  # default is 1 MB for unencoded data-only size.  Setting this number too large
                                         # may cause timeouts or size limit errors.
    concurrent_writes: int = 20


class TornadoWriter(Writer):
    _topic_info: TopicInfo
    _publish_url: str
    _headers: Dict[str, str]
    _client: AsyncHTTPClient
    _options: TornadoWriterOptions

    def __init__(self, topic_info: TopicInfo, client: AsyncHTTPClient, options: TornadoWriterOptions):
        self._topic_info = topic_info
        self._publish_url = topic_info.url("publish")
        self._headers = topic_info.header_map()
        self._client = client
        self._options = options

    async def write(self, to_write: AsyncIterator[UserPubsubMessage]) -> AsyncGenerator[str, UserPubsubMessage]:
        batcher = Batcher(self._options.write_batch_latency_seconds,
                          self._options.write_batch_size_bytes,
                          lambda x: len(x.data))
        batch_gen = batcher.batch(to_write)  # 1 mb limit on batch bytes
        response_gen = parallelizer(batch_gen, self._write_with_retries, self._options.concurrent_writes)
        async for response in response_gen:
            for message_id in response:
                yield message_id

    async def _write_with_retries(self, to_write: List[UserPubsubMessage]) -> Awaitable[List[str]]:
        data = json.dumps({
            "messages": to_write
        }, cls=UserPubsubMessageEncoder)

        publish_request = HTTPRequest(url=self._publish_url, method="POST", body=data, headers=self._headers)

        response: HTTPResponse = await retrying_requester(publish_request, self._client)

        data: Dict[str, List[str]] = json.loads(response.body)

        return data.get("messageIds", [])
