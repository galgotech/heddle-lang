import logging
from typing import Dict, List
from lark import Token, Tree
from lark.visitors import Interpreter

from language.variable_access import VariableAccess
from .memory import Memory
from .prql import Prql
from .value import Value


class PipelineStatement(Interpreter):
    __deep: int
    __memory: Memory
    __modules: Dict
    __result: Dict | List | str | int | float | bool | None

    def __init__(self, deep: int, memory: Memory, modules: Dict):
        self.__deep = deep
        self.__memory = memory
        self.__modules = modules
        self.__result = None

        logging.debug("pipeline_statement", extra={
            "indent": self.__deep,
        })

    @property
    def result(self):
        return self.__result

    def variable_access(self, tree: Tree):
        accessor = VariableAccess(self.__deep + 1, self.__memory)
        accessor.visit(tree)
        self.__result = accessor.result

    def value(self, tree: Tree):
        value_interpreter = Value(self.__deep + 1)
        value_interpreter.visit(tree)
        self.__result = value_interpreter.result

    def pipeline_function_handler(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline function handler")

        if not isinstance(tree.children[1], Token):
            raise Exception("invalid pipeline function handler")

        module_name = tree.children[0].value
        function_name = tree.children[1].value

        module = self.__modules[module_name]
        function = module[function_name]

        if self.__result is not None:
            self.__result = function(self.__result)

    def prql(self, tree: Tree):
        prql_interpreter = Prql(self.__deep + 1, self.__result)
        prql_interpreter.visit(tree)
        self.__result = prql_interpreter.result

    def pipeline_error_handler(self, tree: Tree):
        logging.debug("pipeline_error_handler", extra={
            "indent": self.__deep,
        })
        print(tree)
