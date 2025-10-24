from instructions.instruction import Instruction


class StepUseInstruction(Instruction):
    __step: str

    def __init__(self, step: str) -> None:
        self.__step = step

    @property
    def step(self) -> str:
        return self.__step

    def name(self) -> str:
        return "step_use"
