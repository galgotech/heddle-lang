from ast import Dict
from instructions.instruction import Instruction


class ErrorInstruction(Instruction):
    __error: str
    __config: Dict

    def __init__(self, error: str, config: Dict) -> None:
        super().__init__()
        self.__error = error
        self.__config = config

    @property
    def error(self) -> str:
        return self.__error

    @property
    def config(self) -> Dict:
        return self.__config

    def name(self) -> str:
        return "error"
