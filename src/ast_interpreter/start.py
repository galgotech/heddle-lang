import logging
from lark import Token, Tree
from lark.visitors import Interpreter
from runtime.local import Runtime
from .workflow_definition import WorkflowDefinition


class Start(Interpreter):
    __runtime: Runtime

    def __init__(self, runtime: Runtime):
        super().__init__()
        self.__runtime = runtime

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

    def workflow_definition(self, tree: Tree) -> None:
        WorkflowDefinition(0, self.__runtime).visit(tree)
