from instructions.instruction import Instruction


class ScopeInstruction(Instruction):

    def __init__(self, scope: str) -> None:
        super().__init__()

    def name(self) -> str:
        return "scope"
