from abc import ABC, abstractmethod


class Instruction(ABC):
    @abstractmethod
    def name(self) -> str:
        pass
