from abc import ABC, abstractmethod


class Subscriber(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass
