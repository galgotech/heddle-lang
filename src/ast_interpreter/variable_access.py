import logging
from lark.visitors import Interpreter
import polars as pl

from runtime.local import Runtime


class VariableAccess(Interpreter):
    __deep: int
    __runtime: Runtime
    __result: pl.DataFrame

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime
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
            self.__result = self.__runtime.memory.get(variable, scope)
        except KeyError:
            raise NameError(f"Workflow '{scope}' is not defined")
