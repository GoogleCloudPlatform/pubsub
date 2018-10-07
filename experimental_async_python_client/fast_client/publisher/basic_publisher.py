from asyncio import AbstractEventLoop
from typing import Callable

from tornado.httpclient import AsyncHTTPClient

from fast_client.done_handler import InlineDoneHandler
from fast_client.message_generator import InlineMessageGenerator
from fast_client.publisher import BasePublisher
from fast_client.core import TopicInfo
from fast_client.core.callbacks import PublisherActionT
from fast_client.writer import TornadoWriter, TornadoWriterOptions


class BasicPublisherOptions:
    writer_options: TornadoWriterOptions = TornadoWriterOptions()


class BasicPublisher(BasePublisher):
    def __init__(self, topic: TopicInfo, client: AsyncHTTPClient,
                 publish_action: PublisherActionT, done_action: Callable[[str], None],
                 event_loop: AbstractEventLoop, options=BasicPublisherOptions()):
        try:
            generator = InlineMessageGenerator(publish_action)
            writer = TornadoWriter(topic, client, options.writer_options)
            write_done_handler = InlineDoneHandler(done_action)

            super().__init__(generator, writer, write_done_handler, event_loop)
        except Exception as e:
            print(e)
