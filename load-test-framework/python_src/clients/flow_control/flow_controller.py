from abc import ABC, abstractmethod


class FlowController(ABC):
    @abstractmethod
    def request_start(self):
        pass

    @abstractmethod
    def inform_finished(self, was_successful: bool):
        pass
