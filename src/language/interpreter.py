from typing import Dict
import logging
from lark import Token, Tree
from lark.visitors import Interpreter
from modules import load_module
from .workflow_definition import WorkflowDefinition
from .memory import Memory


class LanguageInterpreter(Interpreter):
    __modules: Dict[str, Dict]
    __memory: Memory

    def __init__(self):
        super().__init__()
        self.__modules = {}
        self.__memory = Memory()

    @property
    def memory(self) -> Memory:
        return self.__memory

    def import_statement(self, tree: Tree) -> None:
        if isinstance(tree.children[0], Token):
            package_name = tree.children[0].value.strip('"')
        else:
            raise Exception("invalid import statment")

        if isinstance(tree.children[1], Token):
            alias = tree.children[1].value
        else:
            raise Exception("invalid import alias")

        logging.debug("import_statement: %s", {"package_name": package_name, "alias": alias})
        self.__modules[alias] = load_module(package_name)

    def workflow_definition(self, tree: Tree) -> None:
        WorkflowDefinition(0, self.__memory, self.__modules).visit(tree)
