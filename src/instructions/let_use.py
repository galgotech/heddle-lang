from instructions.instruction import Instruction


class LetUseInstruction(Instruction):
    __use: str

    def __init__(self, use: str):
        super().__init__()
        self.__use = use

    @property
    def use(self) -> str:
        return self.__use

    def name(self) -> str:
        return "let_use"
