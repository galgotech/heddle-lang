from instructions.instruction import Instruction


class ImportUseInstruction(Instruction):
    def __init__(self, package: str) -> None:
        super().__init__()
        pass

    def name(self) -> str:
        return "import_use"
