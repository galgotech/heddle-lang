from instructions.instruction import Instruction


class ScopeReturnInstruction(Instruction):

    def __init__(self) -> None:
        super().__init__()

    def name(self) -> str:
        return "scope"
