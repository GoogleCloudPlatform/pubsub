import json
from typing import AsyncIterator, AsyncGenerator, List, Dict, Awaitable

from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from fast_client.ack_processor import AckProcessor
from fast_client.helpers import retrying_requester, Batcher, parallelizer
from fast_client.core.subscription_info import SubscriptionInfo


class TornadoAckProcessorOptions:
    ack_batch_latency_seconds: float = 5
    ack_batch_size: int = 1000
    concurrent_acks: int = 20


class TornadoAckProcessor(AckProcessor):
    _subscription: SubscriptionInfo
    _ack_url: str
    _headers: Dict[str, str]
    _client: AsyncHTTPClient
    _options: TornadoAckProcessorOptions

    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient, options: TornadoAckProcessorOptions):
        self._subscription = subscription
        self._ack_url = subscription.url("acknowledge")
        self._headers = subscription.header_map()
        self._client = client
        self._options = options

    async def process_ack(self, ack_ids: AsyncIterator[str]) -> AsyncGenerator[str, str]:
        batcher = Batcher(self._options.ack_batch_latency_seconds, self._options.ack_batch_size)
        batch_gen = batcher.batch(ack_ids)
        ack_gen = parallelizer(batch_gen, self._ack_with_retries, self._options.concurrent_acks)

        async for ack_id_list in ack_gen:
            for ack_id in ack_id_list:
                yield ack_id

    async def _ack_with_retries(self, ids: List[str]) -> Awaitable[List[str]]:
        data = json.dumps({
            "ackIds": ids
        })

        ack_request = HTTPRequest(url=self._ack_url, method="POST", body=data, headers=self._headers)

        response = await retrying_requester(ack_request, self._client)

        return ids
