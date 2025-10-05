import logging
from lark.visitors import Interpreter
import polars as pl
from .memory import Memory


class VariableAccess(Interpreter):
    __deep: int
    __memory: Memory
    __result: pl.DataFrame

    def __init__(self, deep: int, memory: Memory):
        self.__deep = deep
        self.__memory = memory
        self.__result = pl.DataFrame()

    @property
    def result(self):
        return self.__result

    def variable_access(self, tree) -> None:
        scope = tree.children[0].value
        variable = tree.children[1].value

        logging.debug("variable_access: %s", {"scope": scope, "variable": variable}, extra={
            "indent": self.__deep,
        })

        try:
            self.__result = self.__memory.get(variable, scope)
        except KeyError:
            raise NameError(f"Workflow '{scope}' is not defined")
