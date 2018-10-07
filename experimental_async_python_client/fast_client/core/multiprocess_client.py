import asyncio
from abc import ABC
from asyncio import AbstractEventLoop
from concurrent.futures import Future
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Event
from typing import List, Any, Callable

from fast_client.core import Client


class MultiprocessClient(Client, ABC):
    _num_processes: int

    _shutdown: Event = None
    _workers: List[Future] = None

    def __init__(self, num_processes: int):
        self._num_processes = num_processes

    def _start(self, client_factory: Callable[[AbstractEventLoop, Any], Client], *args):
        self._shutdown = Event()
        self._pool = ProcessPoolExecutor(
            max_workers=self._num_processes, initializer=init_shutdown, initargs=(self._shutdown,))
        self._workers = []

        for i in range(self._num_processes):
            print("launching: {}".format(i))
            self._workers.append(self._pool.submit(worker, i, client_factory, *args))

    def stop(self):
        self._shutdown.set()
        for worker_future in self._workers:
            worker_future.result()

    def is_running(self):
        if self._workers is None:
            return False
        for worker_future in self._workers:
            if worker_future.running():
                return True
        return False


shutdown: Event


def init_shutdown(shutdown_input: Event):
    global shutdown
    shutdown = shutdown_input


def worker(worker_id: int, client_factory: Callable[[AbstractEventLoop, Any], Client], *args):
    print("Started worker: {}".format(worker_id))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = client_factory(loop, *args)
    client.start()
    done_fut = asyncio.ensure_future(shutdown_poller(client), loop=loop)
    loop.run_until_complete(done_fut)


async def shutdown_poller(client: Client):
    global shutdown
    while not shutdown.is_set():
        await asyncio.sleep(.5)
    client.stop()
    while client.is_running():
        await asyncio.sleep(.5)
