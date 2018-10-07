from abc import ABC, abstractmethod


class Client(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def is_running(self) -> bool:
        pass
