from instructions.instruction import Instruction


class ErrorUseInstruction(Instruction):
    __error: str

    def __init__(self, error: str) -> None:
        self.__error = error

    @property
    def error(self) -> str:
        return self.__error

    def name(self) -> str:
        return "error_use"
