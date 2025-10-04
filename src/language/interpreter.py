from typing import Dict
from lark.visitors import Interpreter
from .workflow_definition import WorkflowDefinition
from .memory import Memory
from ..modules import load_module


class LanguageInterpreter(Interpreter):
    __modules: Dict
    __memory: Memory

    def __init__(self):
        super().__init__()
        self.__modules = {}
        self.__memory = Memory()

    @property
    def memory(self) -> Memory:
        return self.__memory

    def import_statement(self, tree):
        package_name = tree.children[0].value.strip('"')
        alias = tree.children[1].value
        self.__modules[alias] = load_module(package_name)

    def workflow_definition(self, tree):
        WorkflowDefinition(self.__memory, self.__modules).visit(tree)

    def anonymous_scope(self, tree):
        assert len(tree.children) == 1
        for node in tree.children:
            self.visit(node)
