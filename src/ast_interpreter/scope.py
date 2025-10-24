import logging
from typing import Any, Dict
from lark import Token, Tree
from lark.visitors import Interpreter

from src.ast_interpreter.step_statement import FuncStatement
from runtime.local import Runtime

from .let_statement import LetStatement
from .pipeline_statement import PipelineStatement
from .value import ValueDataFrame


class Scope(Interpreter):
    __deep: int
    __runtime: Runtime
    __modules: Dict
    __result: Any

    def __init__(self, deep: int, runtime: Runtime, modules: Dict):
        self.__deep = deep
        self.__runtime = runtime
        self.__modules = modules
        self.__result = {}
        self.__functions = {}

    @property
    def result(self):
        return self.__result

    def visit(self, tree: Tree) -> Any:
        logging.debug("scope", extra={
            "indent": self.__deep,
        })
        scope_node = tree.children[0]
        if not isinstance(scope_node, Tree):
            raise Exception("invalid scope")

        self.visit_children(tree)

        return self.result

    def func_statement(self, tree: Tree):
        FuncStatement(self.__deep + 1, self.__runtime, self.__modules).run(tree)

    def let_statement(self, tree: Tree):
        interpreter = LetStatement(self.__deep + 1, self.__runtime, self.__modules)
        interpreter.visit(tree)
        self.__result = interpreter.result

    def pipeline_statement(self, tree: Tree):
        interpreter = PipelineStatement(self.__deep + 1, self.__runtime, self.__modules)
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
            self.__result = self.__runtime.memory.get(return_node.value)

        elif return_node.data == "value":
            value_interpreter = ValueDataFrame(self.__deep + 1)
            value_interpreter.visit(return_node)
            self.__result = value_interpreter.result

        elif return_node.data == "pipeline_statement":
            self.__result = PipelineStatement(self.__deep + 1, self.__runtime, self.__modules).visit(return_node)
