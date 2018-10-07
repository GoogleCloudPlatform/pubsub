from asyncio import AbstractEventLoop
from multiprocessing import cpu_count
from typing import Callable

from tornado.httpclient import AsyncHTTPClient

from fast_client.core import MultiprocessClient, TopicInfo
from fast_client.core.callbacks import PublisherActionT
from fast_client.publisher import Publisher, BasicPublisher, BasicPublisherOptions


class MultiprocessPublisherOptions:
    publisher_options: BasicPublisherOptions = BasicPublisherOptions()
    num_processes: int = cpu_count()


class MultiprocessPublisher(Publisher, MultiprocessClient):
    _topic: TopicInfo
    _work: PublisherActionT
    _done_action: Callable[[str], None]
    _options: MultiprocessPublisherOptions

    def __init__(self, topic: TopicInfo, work: PublisherActionT,
                 done_action: Callable[[str], None],
                 options: MultiprocessPublisherOptions = MultiprocessPublisherOptions()):
        self._topic = topic
        self._work = work
        self._done_action = done_action
        self._options = options
        super().__init__(self._options.num_processes)

    def start(self):
        self._start(publisher_factory, self._topic, self._work, self._done_action, self._options.publisher_options)


def publisher_factory(loop: AbstractEventLoop, topic: TopicInfo,
                      work: PublisherActionT, done_action: Callable[[str], None],
                      options: BasicPublisherOptions):
    client = AsyncHTTPClient()
    return BasicPublisher(topic, client, work, done_action, loop, options)
