from typing import Dict
from instructions.instruction import Instruction


class StepInstruction(Instruction):
    __step: str
    __call: str
    __config: Dict

    def __init__(self, step: str, call: str, config: Dict) -> None:
        super().__init__()
        self.__step = step
        self.__call = call
        self.__config = config

    @property
    def step(self) -> str:
        return self.__step

    @property
    def call(self) -> str:
        return self.__call

    @property
    def config(self) -> Dict:
        return self.__config

    def name(self) -> str:
        return "step"
