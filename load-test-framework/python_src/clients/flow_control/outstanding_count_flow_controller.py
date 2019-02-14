from clients.flow_control import FlowController
from threading import Condition
from concurrent.futures import ThreadPoolExecutor, Executor
from cachetools import TTLCache
import sys
import time

expiry_latency_seconds = 15
rate_update_delay_seconds = .1


class OutstandingCountFlowController(FlowController):
    """
    A FlowController that tries to ensure that the outstanding count is roughly equivalent to the
    completion rate in the next two seconds.
    """

    def __init__(self, initial_per_second: float):
        self.rate_per_second = initial_per_second
        self.index = 0
        self.outstanding = 0
        self.condition = Condition()
        self.expiry_cache = TTLCache(sys.maxsize, expiry_latency_seconds)
        self.executor: Executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.reset_rate_in, expiry_latency_seconds)

    def reset_rate_in(self, delay_seconds: float):
        time.sleep(delay_seconds)
        with self.condition:
            self.expiry_cache.expire()
            self.rate_per_second = float(self.expiry_cache.currsize) / expiry_latency_seconds
            self.condition.notify()
        self.reset_rate_in(rate_update_delay_seconds)

    def _num_available(self):
        return int((self.rate_per_second * 2) - self.outstanding)

    def request_start(self):
        with self.condition:
            num_available = self._num_available()
            while num_available < 1:
                self.condition.wait()
                num_available = self._num_available()
            self.outstanding += num_available
            return num_available

    @staticmethod
    def _next_index(index: int):
        return (index + 1) % sys.maxsize

    def inform_finished(self, was_successful: bool):
        with self.condition:
            if was_successful:
                index = self.index
                self.index = self._next_index(index)
                self.expiry_cache[index] = None
            self.outstanding -= 1
            self.condition.notify()
