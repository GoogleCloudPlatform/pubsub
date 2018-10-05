import json
from typing import AsyncIterator, AsyncGenerator, List, Dict, Awaitable
from asyncio import create_task, Future

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError, HTTPResponse

from fast_client.ack_processor import AckProcessor
from fast_client.helpers import retrying_requester
from fast_client.helpers.batcher import batcher
from fast_client.helpers.parallelizer import parallelizer
from fast_client.types.subscription_info import SubscriptionInfo


class TornadoAckProcessor(AckProcessor):
    _subscription: SubscriptionInfo
    _ack_url: str
    _headers: Dict[str, str]
    _client: AsyncHTTPClient

    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient):
        self._subscription = subscription
        self._ack_url = subscription.url("acknowledge")
        self._headers = subscription.header_map()
        self._client = client

    async def process_ack(self, ack_ids: AsyncIterator[str]) -> AsyncGenerator[str, str]:
        batch_gen = batcher(ack_ids, 5, 1000)
        ack_gen = parallelizer(batch_gen, self._ack_task, 10)

        async for ack_id_list in ack_gen:
            for ack_id in ack_id_list:
                yield ack_id

    def _ack_task(self, ids: List[str]) -> "Future[List[str]]":
        try:
            return create_task(self._ack_with_retries(ids))
        except Exception as e:
            print(e)
            raise RuntimeError()

    async def _ack_with_retries(self, ids: List[str]) -> Awaitable[List[str]]:
        data = json.dumps({
            "ackIds": ids
        })

        ack_request = HTTPRequest(url=self._ack_url, method="POST", body=data, headers=self._headers)

        response = await retrying_requester(ack_request, self._client)

        return ids
