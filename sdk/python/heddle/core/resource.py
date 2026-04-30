import abc
from pydantic import BaseModel

class ResourceConfig(BaseModel):
    pass

class Resource(abc.ABC):
    @abc.abstractmethod
    def start(self):
        pass
