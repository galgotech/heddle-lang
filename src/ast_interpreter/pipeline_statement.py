import logging
from lark import Token, Tree
from lark.visitors import Interpreter

from ast_interpreter.variable_access import VariableAccess
from runtime.local import Runtime
from .prql import Prql
from .value import ValueDataFrame


class PipelineStatement(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

        logging.debug("pipeline_statement", extra={
            "indent": self.__deep,
        })

    @property
    def result(self):
        return self.__result

    def variable_access(self, tree: Tree):
        accessor = VariableAccess(self.__deep + 1, self.__runtime)
        accessor.visit(tree)
        self.__result = accessor.result

    def dataframe(self, tree: Tree):
        dataframe_interpreter = ValueDataFrame(self.__deep + 1)
        dataframe_interpreter.visit(tree)
        self.__result = dataframe_interpreter.result

    def import_use(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline function handler")

        if not isinstance(tree.children[1], Token):
            raise Exception("invalid pipeline function handler")
        # if self.__result is not None:
        #     self.__result = function(self.__result)

    def func_use(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline function handler")
        function_name = tree.children[0].value
        self.__runtime.add_stack(function_name)

    def prql(self, tree: Tree):
        prql_interpreter = Prql(self.__deep + 1, self.__result)
        prql_interpreter.visit(tree)
        self.__result = prql_interpreter.result

    def pipeline_error_handler(self, tree: Tree):
        logging.debug("pipeline_error_handler", extra={
            "indent": self.__deep,
        })
        print(tree)
