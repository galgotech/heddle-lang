import logging
from typing import Dict
from lark import Token, Tree
from lark.visitors import Interpreter
import polars as pl

from ast_interpreter.variable_access import VariableAccess
from runtime.local import Runtime
from .prql import Prql
from .value import ValueDataFrame


class PipelineStatement(Interpreter):
    __deep: int
    __runtime: Runtime
    __modules: Dict
    __result: pl.DataFrame
    __data_merge: str

    def __init__(self, deep: int, runtime: Runtime, modules: Dict):
        self.__deep = deep
        self.__runtime = runtime
        self.__modules = modules
        self.__result = pl.DataFrame()

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

    def pipeline_merge_start(self, tree: Tree):
        self.__data_merge = "start"

    def pipeline_merge_end(self, tree: Tree):
        self.__data_merge = "end"

    def import_use(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline function handler")

        if not isinstance(tree.children[1], Token):
            raise Exception("invalid pipeline function handler")

        module_name = tree.children[0].value
        function_name = tree.children[1].value

        module = self.__modules[module_name]
        function = module[function_name]
        self.__runtime.add_stack(function)
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
