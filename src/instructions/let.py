
from instructions.instruction import Instruction


class LetInstruction(Instruction):
    def __init__(self):
        pass

    def name(self) -> str:
        return "let"
