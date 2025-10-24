
from instructions.instruction import Instruction
from .memory import Memory


class Runtime:
    __memory: Memory
    __stack: list

    def __init__(self) -> None:
        self.__memory = Memory()
        self.__stack = []

    @property
    def memory(self) -> Memory:
        return self.__memory

    def add_stack(self, func: Instruction):
        self.__stack.append(func)
