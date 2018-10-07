import asyncio
from asyncio import AbstractEventLoop, Event, run_coroutine_threadsafe
from concurrent.futures import Future

from fast_client.done_handler import DoneHandler
from fast_client.message_generator import MessageGenerator
from fast_client.publisher import Publisher
from fast_client.writer import Writer


class BasePublisher(Publisher):
    _message_generator: MessageGenerator
    _writer: Writer
    _done_handler: DoneHandler
    _event_loop: AbstractEventLoop

    _shutdown: Event = None
    _completed: Future = None

    def __init__(self, message_generator: MessageGenerator, writer: Writer,
                 done_handler: DoneHandler, event_loop: AbstractEventLoop):
        self._message_generator = message_generator
        self._writer = writer
        self._done_handler = done_handler
        self._event_loop = event_loop

    def start(self):
        self._shutdown = Event()
        message_gen = self._message_generator.generate(self._shutdown)
        writer_gen = self._writer.write(message_gen)
        done_coro = self._done_handler.done(writer_gen)

        self._completed = run_coroutine_threadsafe(done_coro, self._event_loop)

    def stop(self):
        asyncio.run_coroutine_threadsafe(self._set_shutdown(), self._event_loop)

    async def _set_shutdown(self):
        self._shutdown.set()

    def is_running(self) -> bool:
        if self._completed is None:
            return False
        return self._completed.running()
