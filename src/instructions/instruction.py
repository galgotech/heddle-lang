from abc import ABC, abstractmethod
import uuid


class Instruction(ABC):
    __id: uuid.UUID

    def __init__(self) -> None:
        self.__id = uuid.uuid4()

    @property
    def id(self) -> uuid.UUID:
        return self.__id

    @abstractmethod
    def name(self) -> str:
        pass
