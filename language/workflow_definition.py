from typing import Dict
from lark.visitors import Interpreter

from .anonymous_scope import AnonymousScope
from .let_statement import LetStatement
from .memory import Memory

class WorkflowDefinition(Interpreter):
    __modules: Dict
    __memory: Memory
    __name: str

    def __init__(self, memory: Memory, modules: Dict):
        self.__memory = memory
        self.__modules = modules
        self.__name = None

    def visit(self, tree):
        self.__name = tree.children[0].value

        self.__memory.enter_scope()
        self.visit_children(tree)

        workflow_scope = self.__memory.current_scope
        self.__memory.exit_scope()

        self.__memory.set(self.__name, workflow_scope)

    def let_statement(self, tree):
        let_statement = LetStatement(self.__memory, self.__modules)
        let_statement.visit(tree)

    def anonymous_scope(self, tree):
        scope = AnonymousScope(self.__memory, self.__modules)
        scope.visit(tree)
