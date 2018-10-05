import asyncio
from concurrent.futures import Future, ProcessPoolExecutor, Executor
from multiprocessing import cpu_count, Event
from typing import List

from tornado.httpclient import AsyncHTTPClient

from fast_client.processor.inline_processor import ActionT
from fast_client.subscriber import Subscriber, BasicSubscriber
from fast_client.types.subscription_info import SubscriptionInfo


class MultiprocessSubscriber(Subscriber):
    _subscription: SubscriptionInfo
    _work: ActionT
    _num_processes: int

    _shutdown: Event
    _pool: Executor
    _workers: List[Future]

    def __init__(self, subscription: SubscriptionInfo, work: ActionT, num_processes: int = cpu_count()):
        self._subscription = subscription
        self._work = work
        self._num_processes = num_processes

    def start(self):
        self._shutdown = Event()
        self._pool = ProcessPoolExecutor(
            max_workers=self._num_processes, initializer=init_shutdown, initargs=(self._shutdown, ))
        self._workers = []

        for i in range(self._num_processes):
            print("launching: {}".format(i))
            self._workers.append(self._pool.submit(subscriber_worker, self._subscription, self._work, i))

    def stop(self):
        self._shutdown.set()
        for worker in self._workers:
            worker.result()


shutdown: Event


def init_shutdown(shutdown_input: Event):
    global shutdown
    shutdown = shutdown_input


def subscriber_worker(subscription: SubscriptionInfo, work: ActionT, worker_id: int):
    print("Started worker: {}".format(worker_id))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = AsyncHTTPClient()
    subscriber = BasicSubscriber(subscription, client, work, loop)
    subscriber.start()
    done_fut = asyncio.ensure_future(shutdown_poller(subscriber), loop=loop)
    loop.run_until_complete(done_fut)


async def shutdown_poller(subscriber: Subscriber):
    global shutdown
    while not shutdown.is_set():
        await asyncio.sleep(1)
    await asyncio.wrap_future(subscriber.stop())
