import logging
from lark import Token, Tree
from lark.visitors import Interpreter
from ast_interpreter.error_statement import ErrorStatement
from ast_interpreter.schema_statement import SchemaStatement
from ast_interpreter.step_statement import StepStatement
from instructions.import_ import ImportInstruction
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
        self.__runtime.add_stack(ImportInstruction(package_name, alias))

    def schema_statement(self, tree: Tree) -> None:
        SchemaStatement(0, self.__runtime).run(tree)
        # self.__runtime.add_stack(SchemaInstruction())

    def step_statement(self, tree: Tree) -> None:
        StepStatement(0, self.__runtime).run(tree)

    def error_statement(self, tree: Tree) -> None:
        ErrorStatement(0, self.__runtime).run(tree)

    def workflow_definition(self, tree: Tree) -> None:
        WorkflowDefinition(0, self.__runtime).run(tree)
