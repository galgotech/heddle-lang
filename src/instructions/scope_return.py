from instructions.instruction import Instruction


class ScopeReturnInstruction(Instruction):

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "scope"
