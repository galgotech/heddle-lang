import logging
from typing import Dict
import polars as pl
from lark import Token
from lark.visitors import Interpreter

from .memory import Memory
from .variable_access import VariableAccess
from .value import ValueDataFrame
from .pipeline_statement import PipelineStatement


class LetStatement(Interpreter):
    __deep: int
    __modules: Dict
    __memory: Memory
    __name: str
    __value: pl.DataFrame

    def __init__(self, deep: int, memory, modules):
        self.__deep = deep
        self.__memory = memory
        self.__modules = modules
        self.__name = ""
        self.__value = pl.DataFrame()

    @property
    def result(self) -> pl.DataFrame:
        return self.__value

    def visit(self, tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid let name")

        self.__name = nameChild.value

        logging.debug("let_statement: %s", {"name": self.__name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)

        self.__memory.set(self.__name, self.__value)

    def let_expression(self, tree):
        expression_node = tree.children[0]

        if expression_node.data == "dataframe":
            value_interpreter = ValueDataFrame(self.__deep + 1)
            value_interpreter.visit(expression_node)
            self.__value = value_interpreter.result

        elif expression_node.data == "pipeline_statement":
            pipeline_interpreter = PipelineStatement(self.__deep + 1, self.__memory, self.__modules)
            pipeline_interpreter.visit(expression_node)
            self.__value = pipeline_interpreter.result

        elif expression_node.data == "variable_access":
            variable_access = VariableAccess(self.__deep + 1, self.__memory)
            variable_access.visit(expression_node)
            self.__value = variable_access.result

        else:
            raise Exception("not implemented '%s'" % expression_node.data)

        logging.debug("let_expression: %s", {"value": self.__value}, extra={
            "indent": self.__deep,
        })
