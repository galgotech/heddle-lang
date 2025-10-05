import logging
from typing import Dict, List
from lark.visitors import Interpreter
from .memory import Memory


class VariableAccess(Interpreter):
    __deep: int
    __memory: Memory
    __result: Dict | List | str | int | float | bool | None

    def __init__(self, deep: int, memory: Memory):
        self.__deep = deep
        self.__memory = memory
        self.__result = None

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
