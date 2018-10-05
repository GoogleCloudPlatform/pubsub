from asyncio import AbstractEventLoop

from tornado.httpclient import AsyncHTTPClient

from fast_client.ack_done_handler import NoopAckDoneHandler
from fast_client.ack_processor import TornadoAckProcessor
from fast_client.processor import InlineProcessor
from fast_client.processor.inline_processor import ActionT
from fast_client.reader import TornadoReader
from fast_client.subscriber import BaseSubscriber
from fast_client.types.subscription_info import SubscriptionInfo


class BasicSubscriber(BaseSubscriber):
    _subscription: SubscriptionInfo
    _client: AsyncHTTPClient

    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient,
                 work: ActionT, event_loop: AbstractEventLoop):
        reader = TornadoReader(subscription, client, 10)
        processor = InlineProcessor(work)
        ack_processor = TornadoAckProcessor(subscription, client)
        ack_done_handler = NoopAckDoneHandler()
        super().__init__(reader, processor, ack_processor, ack_done_handler, event_loop)
