from typing import Dict
from instructions.instruction import Instruction


class SchemaInstruction(Instruction):
    __schema: str
    __config: Dict

    def __init__(self, schema: str, config: Dict) -> None:
        self.__schema = schema
        self.__config = config

    @property
    def schema(self) -> str:
        return self.__schema

    @property
    def config(self) -> Dict:
        return self.__config

    def name(self) -> str:
        return "schema"
