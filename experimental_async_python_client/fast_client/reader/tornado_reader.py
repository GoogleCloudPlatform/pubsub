import base64
import json
import ciso8601
from asyncio import Event
from typing import AsyncIterator, Dict, List, Awaitable

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from fast_client.helpers import parallelizer
from fast_client.core import PubsubMessage
from fast_client.reader import Reader
from fast_client.core.subscription_info import SubscriptionInfo


class TornadoReaderOptions:
    concurrent_reads: int = 20
    max_messages: int = 1000


class TornadoReader(Reader):
    _subscription: SubscriptionInfo
    _sub_request: HTTPRequest
    _client: AsyncHTTPClient
    _options: TornadoReaderOptions

    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient, options: TornadoReaderOptions):
        self._subscription = subscription
        sub_url: str = subscription.url("pull")
        sub_body: str = json.dumps({
            "maxMessages": options.max_messages,
            "returnImmediately": True
        })
        self._sub_request = HTTPRequest(url=sub_url, method="POST", body=sub_body, headers=subscription.header_map())

        self._client = client
        self._options = options

    async def read(self, shutdown: Event) -> AsyncIterator[PubsubMessage]:
        request_gen = self._request_stream(shutdown)
        response_gen = parallelizer(request_gen, self._request_once, self._options.concurrent_reads)
        async for response in response_gen:
            if response.code != 200:
                print(response.error)
                print(response.reason)
                print(response.body)
                continue

            results: Dict[str, List[Dict]]
            try:
                results = json.loads(response.body)
            except ValueError as e:
                print(e)
                continue

            if "receivedMessages" not in results:
                continue

            for result in results["receivedMessages"]:
                try:
                    message = self._parse_result(result)
                    yield message
                except PubsubError as e:
                    print(e)
                    continue

    async def _request_stream(self, shutdown: Event) -> AsyncIterator[HTTPRequest]:
        while not shutdown.is_set():
            yield self._sub_request
        return

    async def _request_once(self, request: HTTPRequest) -> Awaitable[HTTPResponse]:
        return await self._client.fetch(request, raise_error=False)

    @staticmethod
    def _parse_result(result: dict) -> PubsubMessage:
        if "ackId" not in result or "message" not in result:
            raise PubsubError("Invalid pubsub message received: {}".format(result))

        raw_message = result["message"]
        if ("data" not in raw_message
                or "messageId" not in raw_message
                or "publishTime" not in raw_message):
            raise PubsubError("Invalid pubsub message received: {}".format(result))

        data = base64.decodebytes(raw_message["data"].encode("utf-8"))
        publishTime = ciso8601.parse_datetime(raw_message["publishTime"])

        return PubsubMessage(
            data, raw_message.get("attributes", {}), raw_message["messageId"], publishTime, result["ackId"])


class PubsubError(BaseException):
    pass
