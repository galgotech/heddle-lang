import logging
from lark import Token, Tree
from lark.visitors import Interpreter

from src.ast_interpreter.step_statement import StepStatement
from runtime.local import Runtime

from .let_statement import LetStatement
from .pipeline_statement import PipelineStatement
from .value import ValueDataFrame


class Scope(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def visit(self, tree: Tree):
        logging.debug("scope", extra={
            "indent": self.__deep,
        })
        scope_node = tree.children[0]
        if not isinstance(scope_node, Tree):
            raise Exception("invalid scope")

        self.visit_children(tree)

    def step_statement(self, tree: Tree):
        StepStatement(self.__deep + 1, self.__runtime).run(tree)

    def let_statement(self, tree: Tree):
        interpreter = LetStatement(self.__deep + 1, self.__runtime)
        interpreter.visit(tree)
        self.__result = interpreter.result

    def pipeline_statement(self, tree: Tree):
        interpreter = PipelineStatement(self.__deep + 1, self.__runtime)
        self.__result = interpreter.visit(tree)

    def scope_statement(self, tree: Tree):
        logging.debug("scope_statement", extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)

    def scope_return(self, tree: Tree):
        logging.debug("scope_return", extra={
            "indent": self.__deep,
        })
        return_node = tree.children[0]
        if not isinstance(return_node, Tree):
            raise Exception("invalid scope return")

        if isinstance(return_node, Token) and return_node.type == "VARIABLE_NAME":
            self.__runtime.memory.get(return_node.value)

        elif return_node.data == "value":
            value_interpreter = ValueDataFrame(self.__deep + 1)
            value_interpreter.visit(return_node)

        elif return_node.data == "pipeline_statement":
            PipelineStatement(self.__deep + 1, self.__runtime,).visit(return_node)
