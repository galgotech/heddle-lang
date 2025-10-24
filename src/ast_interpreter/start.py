from typing import Dict
import logging
from lark import Token, Tree
from lark.visitors import Interpreter
from modules import load_module
from runtime.local import Runtime
from .workflow_definition import WorkflowDefinition


class Start(Interpreter):
    __runtime: Runtime
    __modules: Dict[str, Dict]

    def __init__(self, runtime: Runtime):
        super().__init__()
        self.__runtime = runtime
        self.__modules = {}

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
        WorkflowDefinition(0, self.__runtime, self.__modules).visit(tree)
