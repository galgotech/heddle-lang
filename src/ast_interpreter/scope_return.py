import logging
from lark import Tree
from lark.visitors import Interpreter

from ast_interpreter.let_use import LetUse
from ast_interpreter.pipeline_statement import PipelineStatement
from ast_interpreter.value import ValueDataFrame
from runtime.local import Runtime


class ScopeReturn(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree):
        logging.debug("scope_return", extra={
            "indent": self.__deep,
        })
        self.visit_children(tree)

    def let_use(self, tree: Tree):
        LetUse(self.__deep + 1, self.__runtime).run(tree)

    def dataframe(self, tree: Tree):
        ValueDataFrame(self.__deep + 1).run(tree)

    def pipeline_statement(self, tree: Tree):
        PipelineStatement(self.__deep + 1, self.__runtime,).run(tree)
