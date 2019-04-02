from abc import ABC, abstractmethod


class FlowController(ABC):
    @abstractmethod
    def request_start(self):
        """Request to start an operation

        :return: the number of currently allowed operations. Block until an
        operation is allowed.
        """
        pass

    @abstractmethod
    def inform_finished(self, was_successful: bool):
        pass
