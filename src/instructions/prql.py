
from instructions.instruction import Instruction


class PrqlInstruction(Instruction):
    _query: str

    def __init__(self, query: str):
        self._query = query

    @property
    def query(self) -> str:
        return self._query

    def name(self) -> str:
        return "prql"
