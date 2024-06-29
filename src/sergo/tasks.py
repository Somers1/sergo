from abc import ABC, abstractmethod


class Task(ABC):
    @abstractmethod
    def perform_task(self, event):
        pass
