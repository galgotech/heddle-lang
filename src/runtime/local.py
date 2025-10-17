
from typing import Dict
from runtime.func_config import FuncConfig
from .memory import Memory


class Runtime:
    __memory: Memory
    __functions: Dict[str, FuncConfig]
    __opeations: list

    def __init__(self) -> None:
        self.__memory = Memory()
        self.__functions = {}
        self.__stack = []

    @property
    def memory(self) -> Memory:
        return self.__memory

    def add_function(self, name: str, config: FuncConfig):
        self.__functions[name] = config

    def _has_function(self, name: str) -> bool:
        return name in self.__functions

    def add_stack(self, func: str):
        self.__stack.append(func)
