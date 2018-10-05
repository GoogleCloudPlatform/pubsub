import asyncio
from asyncio import AbstractEventLoop, Event
from concurrent.futures import Future

from fast_client.ack_done_handler import AckDoneHandler
from fast_client.ack_processor import AckProcessor
from fast_client.processor import Processor
from fast_client.reader import Reader
from fast_client.subscriber import Subscriber


class BaseSubscriber(Subscriber):
    _reader: Reader
    _processor: Processor
    _ack_processor: AckProcessor
    _ack_done_handler: AckDoneHandler
    _event_loop: AbstractEventLoop

    _shutdown: Event
    _completed: Future

    def __init__(self, reader: Reader, processor: Processor, ack_processor: AckProcessor,
                 ack_done_handler: AckDoneHandler, event_loop: AbstractEventLoop):
        self._reader = reader
        self._processor = processor
        self._ack_processor = ack_processor
        self._ack_done_handler = ack_done_handler
        self._event_loop = event_loop

    def start(self):
        self._shutdown = Event()
        read_gen = self._reader.read(self._shutdown)
        process_gen = self._processor.process(read_gen)
        ack_processor_gen = self._ack_processor.process_ack(process_gen)
        ack_done_coro = self._ack_done_handler.ack_done(ack_processor_gen)

        self._completed = asyncio.run_coroutine_threadsafe(ack_done_coro, self._event_loop)

    def stop(self):
        asyncio.run_coroutine_threadsafe(self._shutdown.set(), self._event_loop).result()
        self._completed.result()
