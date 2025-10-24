import logging
from lark import Token, Tree
from lark.visitors import Interpreter

from runtime.local import Runtime

from .workflow_access_let_use import WorkflowAccessLetUse
from .value import ValueDataFrame
from .pipeline_statement import PipelineStatement


class LetStatement(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid let name")

        name = nameChild.value
        logging.debug("let_statement: %s", {"name": name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)

    def pipeline_statement(self, tree: Tree):
        PipelineStatement(self.__deep + 1, self.__runtime).run(tree)

    def dataframe(self, tree: Tree):
        ValueDataFrame(self.__deep + 1, self.__runtime).run(tree)

    def let_use(self, tree: Tree):
        WorkflowAccessLetUse(self.__deep + 1, self.__runtime).run(tree)
