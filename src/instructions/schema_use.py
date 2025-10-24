from dis import Instruction


class SchemaUseInstruction(Instruction):
    def __init__(self, schema: str):
        self.__schema = schema

    @property
    def schema(self) -> str:
        return self.__schema

    def name(self) -> str:
        return "schema_use"

