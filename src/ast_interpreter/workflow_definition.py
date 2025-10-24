import logging
from lark import Token
from lark.visitors import Interpreter

from instructions.scope import ScopeInstruction
from runtime.local import Runtime

from .scope import Scope


class WorkflowDefinition(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree):
        name = None
        if isinstance(tree.children[0], Token):
            assert tree.children[0].type == "IDENTIFIER"
            name = tree.children[0].value
        else:
            raise Exception("invalid workflow name")

        logging.debug("workflow_definition: %s", {"name": name})

        self.__runtime.memory.enter_scope(name)
        self.__runtime.add_stack(ScopeInstruction(name))
        self.visit_children(tree)

    def scope(self, tree):
        Scope(self.__deep + 1, self.__runtime).run(tree)
