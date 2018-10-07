from typing import AsyncIterator, Awaitable, Callable

from fast_client.done_handler import DoneHandler


class InlineDoneHandler(DoneHandler):
    _action: Callable[[str], None]

    def __init__(self, action: Callable[[str], None]):
        self._action = action

    async def done(self, completed_ids: AsyncIterator[str]) -> Awaitable[None]:
        async for completed_id in completed_ids:
            try:
                self._action(completed_id)
            except Exception as e:
                print("user work exception: {}".format(e))
