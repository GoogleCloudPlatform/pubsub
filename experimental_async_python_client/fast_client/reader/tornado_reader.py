import base64
import json
from dateutil import parser
from asyncio import Event, Task, Future
from typing import AsyncIterator, Set, Dict, List

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from fast_client.helpers import parallelizer
from fast_client.types import PubsubMessage
from fast_client.reader import Reader
from fast_client.types.subscription_info import SubscriptionInfo


class TornadoReader(Reader):
    _subscription: SubscriptionInfo
    _sub_request: HTTPRequest
    _client: AsyncHTTPClient
    _concurrent_reads: int

    _outstanding: Set[Task] = set()

    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient, concurrent_reads: int):
        self._subscription = subscription
        sub_url: str = subscription.url("pull")
        sub_body: str = json.dumps({
            "maxMessages": 1000,
            "returnImmediately": True
        })
        self._sub_request = HTTPRequest(url=sub_url, method="POST", body=sub_body, headers=subscription.header_map())

        self._client = client
        self._concurrent_reads = concurrent_reads

    async def read(self, shutdown: Event) -> AsyncIterator[PubsubMessage]:
        request_gen = self._request_stream(shutdown)
        response_gen = parallelizer(request_gen, self._request_once, self._concurrent_reads)
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

    def _request_once(self, request: HTTPRequest) -> "Future[HTTPResponse]":
        return self._client.fetch(request, raise_error=False)

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
        publishTime = parser.parse(raw_message["publishTime"])

        return PubsubMessage(
            data, raw_message.get("attributes", {}), raw_message["messageId"], publishTime, result["ackId"])


class PubsubError(BaseException):
    pass
