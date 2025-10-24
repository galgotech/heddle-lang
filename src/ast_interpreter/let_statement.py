import logging
from typing import Dict
import polars as pl
from lark import Token
from lark.visitors import Interpreter

from runtime.local import Runtime

from .variable_access import VariableAccess
from .value import ValueDataFrame
from .pipeline_statement import PipelineStatement


class LetStatement(Interpreter):
    __deep: int
    __modules: Dict
    __runtime: Runtime
    __value: pl.DataFrame

    def __init__(self, deep: int, runtime: Runtime, modules):
        self.__deep = deep
        self.__runtime = runtime
        self.__modules = modules
        self.__value = pl.DataFrame()

    def visit(self, tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid let name")

        name = nameChild.value
        logging.debug("let_statement: %s", {"name": name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)

        self.__runtime.memory.set(name, self.__value)

    def let_expression(self, tree):
        expression_node = tree.children[0]

        if expression_node.data == "dataframe":
            value_interpreter = ValueDataFrame(self.__deep + 1)
            value_interpreter.visit(expression_node)
            self.__value = value_interpreter.result

        elif expression_node.data == "pipeline_statement":
            PipelineStatement(self.__deep + 1, self.__runtime, self.__modules).visit(expression_node)

        elif expression_node.data == "variable_access":
            VariableAccess(self.__deep + 1, self.__runtime).visit(expression_node)

        else:
            raise Exception("not implemented '%s'" % expression_node.data)

        logging.debug("let_expression: %s", {"value": self.__value}, extra={
            "indent": self.__deep,
        })
