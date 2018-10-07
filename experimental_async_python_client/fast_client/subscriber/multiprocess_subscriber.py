from asyncio import AbstractEventLoop
from multiprocessing import cpu_count
from typing import Callable

from tornado.httpclient import AsyncHTTPClient

from fast_client.subscriber import Subscriber, BasicSubscriber, BasicSubscriberOptions
from fast_client.core.callbacks import SubscriberActionT
from fast_client.core import SubscriptionInfo, MultiprocessClient


class MultiprocessSubscriberOptions:
    subscriber_options: BasicSubscriberOptions = BasicSubscriberOptions()
    num_processes: int = cpu_count()


class MultiprocessSubscriber(Subscriber, MultiprocessClient):
    _subscription: SubscriptionInfo
    _work: SubscriberActionT
    _done_action: Callable[[str], None]
    _options: MultiprocessSubscriberOptions

    def __init__(self, subscription: SubscriptionInfo, work: SubscriberActionT,
                 done_action: Callable[[str], None],
                 options: MultiprocessSubscriberOptions = MultiprocessSubscriberOptions()):
        self._subscription = subscription
        self._work = work
        self._done_action = done_action
        self._options = options
        super().__init__(self._options.num_processes)

    def start(self):
        self._start(subscriber_factory, self._subscription, self._work,
                    self._done_action, self._options.subscriber_options)


def subscriber_factory(loop: AbstractEventLoop, subscription: SubscriptionInfo,
                       work: SubscriberActionT, done_action: Callable[[str], None],
                       options: BasicSubscriberOptions):
    client = AsyncHTTPClient()
    return BasicSubscriber(subscription, client, work, done_action, loop, options)
