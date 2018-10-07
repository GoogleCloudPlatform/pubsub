import asyncio
from asyncio import AbstractEventLoop, Event
from concurrent.futures import Future

from fast_client.done_handler import DoneHandler
from fast_client.ack_processor import AckProcessor
from fast_client.processor import Processor
from fast_client.reader import Reader
from fast_client.subscriber import Subscriber


class BaseSubscriber(Subscriber):
    _reader: Reader
    _processor: Processor
    _ack_processor: AckProcessor
    _done_handler: DoneHandler
    _event_loop: AbstractEventLoop

    _shutdown: Event = None
    _completed: Future = None

    def __init__(self, reader: Reader, processor: Processor, ack_processor: AckProcessor,
                 done_handler: DoneHandler, event_loop: AbstractEventLoop):
        self._reader = reader
        self._processor = processor
        self._ack_processor = ack_processor
        self._done_handler = done_handler
        self._event_loop = event_loop

    def start(self):
        self._shutdown = Event()
        read_gen = self._reader.read(self._shutdown)
        process_gen = self._processor.process(read_gen)
        ack_processor_gen = self._ack_processor.process_ack(process_gen)
        done_coro = self._done_handler.done(ack_processor_gen)

        self._completed = asyncio.run_coroutine_threadsafe(done_coro, self._event_loop)

    def stop(self):
        asyncio.run_coroutine_threadsafe(self._set_shutdown(), self._event_loop)

    async def _set_shutdown(self):
        self._shutdown.set()

    def is_running(self) -> bool:
        if self._completed is None:
            return False
        return self._completed.running()
