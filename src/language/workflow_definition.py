import logging
from typing import Dict
from lark import Token
from lark.visitors import Interpreter

from .scope import Scope
from .memory import Memory


class WorkflowDefinition(Interpreter):
    __deep: int
    __modules: Dict
    __memory: Memory
    __name: str

    def __init__(self, deep: int, memory: Memory, modules: Dict):
        self.__deep = deep
        self.__memory = memory
        self.__modules = modules
        self.__name = ""

    def visit(self, tree):
        if isinstance(tree.children[0], Token):
            self.__name = tree.children[0].value
        else:
            raise Exception("invalid workflow name")

        logging.debug("workflow_definition: %s", {"name": self.__name})

        self.__memory.enter_scope(self.__name)
        self.visit_children(tree)

    def scope(self, tree):
        interpreter = Scope(self.__deep + 1, self.__memory, self.__modules)
        interpreter.run(tree)
