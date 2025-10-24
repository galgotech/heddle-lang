import logging
from typing import Dict
from lark import Token
from lark.visitors import Interpreter

from runtime.func_config import FuncConfig
from runtime.local import Runtime
from .value import ValueDict


class StepStatement(Interpreter):
    __deep: int
    __runtime: Runtime
    __name: str
    __call: str
    __call_config: Dict

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime
        self.__name = ""

    def run(self, tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid func name")

        self.__name = nameChild.value

        logging.debug("func_statement: %s", {"name": self.__name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)

        self.__runtime.add_function(self.__name, FuncConfig(self.__call, self.__call_config))

    def import_use(self, tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid import name")

        logging.debug("import_use: %s", {"value": nameChild.value}, extra={
            "indent": self.__deep,
        })

        self.__call = nameChild.value

    def func_expression_config(self, tree):
        config = ValueDict(self.__deep + 1)
        config.visit(tree.children[0])
        self.__call_config = config.result
