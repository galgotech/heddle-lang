
from instructions.instruction import Instruction


class ImportInstruction(Instruction):
    __package: str
    __alias: str

    def __init__(self, package: str, alias: str) -> None:
        self.__package = package
        self.__alias = alias

    @property
    def package(self) -> str:
        return self.__package

    @property
    def alias(self) -> str:
        return self.__alias

    def name(self) -> str:
        return "import"
