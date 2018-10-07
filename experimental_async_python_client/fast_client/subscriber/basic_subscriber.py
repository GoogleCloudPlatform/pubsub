from asyncio import AbstractEventLoop
from typing import Callable

from tornado.httpclient import AsyncHTTPClient

from fast_client.done_handler import InlineDoneHandler
from fast_client.ack_processor import TornadoAckProcessor, TornadoAckProcessorOptions
from fast_client.processor import InlineProcessor
from fast_client.reader import TornadoReader, TornadoReaderOptions
from fast_client.subscriber import BaseSubscriber
from fast_client.core.callbacks import SubscriberActionT
from fast_client.core import SubscriptionInfo


class BasicSubscriberOptions:
    reader_options: TornadoReaderOptions = TornadoReaderOptions()
    ack_processor_options: TornadoAckProcessorOptions = TornadoAckProcessorOptions()


class BasicSubscriber(BaseSubscriber):
    def __init__(self, subscription: SubscriptionInfo, client: AsyncHTTPClient,
                 work: SubscriberActionT, done_action: Callable[[str], None],
                 event_loop: AbstractEventLoop, options: BasicSubscriberOptions = BasicSubscriberOptions()):
        reader = TornadoReader(subscription, client, options.reader_options)
        processor = InlineProcessor(work)
        ack_processor = TornadoAckProcessor(subscription, client, options.ack_processor_options)
        ack_done_handler = InlineDoneHandler(done_action)
        super().__init__(reader, processor, ack_processor, ack_done_handler, event_loop)
