import logging
from typing import Dict
from lark import Token
from lark.visitors import Interpreter

from runtime.local import Runtime

from .scope import Scope


class WorkflowDefinition(Interpreter):
    __deep: int
    __runtime: Runtime
    __modules: Dict
    __name: str

    def __init__(self, deep: int, runtime: Runtime, modules: Dict):
        self.__deep = deep
        self.__runtime = runtime
        self.__modules = modules
        self.__name = ""

    def run(self, tree):
        if isinstance(tree.children[0], Token):
            assert tree.children[0].type == "IDENTIFIER"
            self.__name = tree.children[0].value
        else:
            raise Exception("invalid workflow name")

        logging.debug("workflow_definition: %s", {"name": self.__name})

        self.__runtime.memory.enter_scope(self.__name)
        self.visit_children(tree)

    def scope(self, tree):
        interpreter = Scope(self.__deep + 1, self.__runtime, self.__modules)
        interpreter.run(tree)
