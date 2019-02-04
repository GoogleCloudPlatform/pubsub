from clients.flow_control import FlowController
from concurrent.futures import Executor, ThreadPoolExecutor
import time
from threading import Condition


class RateLimiterFlowController(FlowController):
    """
    A FlowController that allows actions at a given per second rate.
    """
    def __init__(self, per_second_rate: float):
        self.seconds_per_token = 1/per_second_rate
        self.tokens = 0
        self.condition = Condition()
        self.executor: Executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self._token_generator)

    def _token_generator(self):
        time.sleep(self.seconds_per_token)
        with self.condition:
            self.tokens += 1
            self.condition.notify()
        self._token_generator()

    def request_start(self):
        with self.condition:
            while self.tokens < 1:
                self.condition.wait()
            self.tokens -= 1

    def inform_finished(self, was_successful: bool):
        pass
