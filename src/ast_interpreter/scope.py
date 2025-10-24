import logging
from lark import Tree
from lark.visitors import Interpreter

from ast_interpreter.scope_return import ScopeReturn
from runtime.local import Runtime

from .let_statement import LetStatement
from .pipeline_statement import PipelineStatement


class Scope(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree):
        logging.debug("scope", extra={
            "indent": self.__deep,
        })
        self.visit_children(tree)

    def let_statement(self, tree: Tree):
        LetStatement(self.__deep + 1, self.__runtime).run(tree)

    def pipeline_statement(self, tree: Tree):
        PipelineStatement(self.__deep + 1, self.__runtime).run(tree)

    def scope_return(self, tree: Tree):
        ScopeReturn(self.__deep + 1, self.__runtime).run(tree)
