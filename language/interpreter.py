from typing import Dict
from lark.visitors import Interpreter
from .workflow_definition import WorkflowDefinition
from .memory import Memory
from .mock_modules import get_mock_module


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
        assert len(tree.children) == 2
        assert tree.children[0].type == "IMPORT_PACKAGE"
        assert tree.children[1].type == "IMPORT_ALIAS"
        alias = tree.children[1].value
        self.__modules[alias] = get_mock_module(alias)

    def workflow_definition(self, tree):
        WorkflowDefinition(self.__memory, self.__modules).visit(tree)
        
    def anonymous_scope(self, tree):
        assert len(tree.children) == 1
        for node in tree.children:
            self.visit(node)

