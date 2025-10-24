
from instructions.instruction import Instruction


class LetInstruction(Instruction):
    def __init__(self):
        super().__init__()

    def name(self) -> str:
        return "let"
