from typing import Dict
from lark import Token, Tree
from lark.visitors import Interpreter

from .let_statement import LetStatement
from .memory import Memory
from .pipeline_statement import PipelineStatement
from .value import Value


class AnonymousScope(Interpreter):
    __memory: Memory
    __modules: Dict
    __result: any

    def __init__(self, memory: Memory, modules: Dict):
        self.__memory = memory
        self.__modules = modules
        self.__result = {}

    @property
    def result(self):
        return self.__result

    def run(self, tree: Tree):
        scope_node = tree.children[0]
        self.__memory.enter_scope()
        for child in scope_node.children:
            self.visit(child)
        self.__memory.exit_scope()
        return self.result

    def let_statement(self, tree: Tree):
        interpreter = LetStatement(self.__memory, self.__modules)
        interpreter.visit(tree)
        self.__result = interpreter.result

    def pipeline_statement(self, tree: Tree):
        interpreter = PipelineStatement(self.__memory, self.__modules)
        self.__result = interpreter.visit(tree)

    def anonymous_scope(self, tree: Tree):
        interpreter = AnonymousScope(self.__memory, self.__modules)
        self.__result = interpreter.run(tree)

    def scope_statement(self, tree: Tree):
        for child in tree.children:
            self.visit(child)

    def scope_return(self, tree: Tree):
        return_node = tree.children[0]

        if isinstance(return_node, Token) and return_node.type == "VARIABLE_NAME":
            self.__result = self.__memory.get(return_node.value)

        elif return_node.data == "value":
            value_interpreter = Value()
            value_interpreter.visit(return_node)
            self.__result = value_interpreter.result

        elif return_node.data == "pipeline_statement":
            self.__result = PipelineStatement(
                self.__memory, self.__modules
            ).visit(return_node)
