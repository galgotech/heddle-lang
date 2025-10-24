import logging
from lark import Token, Tree
from lark.visitors import Interpreter

from ast_interpreter.workflow_access_let_use import WorkflowAccessLetUse
from instructions.error_use import ErrorUseInstruction
from instructions.let_use import LetUseInstruction
from instructions.step_use import StepUseInstruction
from runtime.local import Runtime
from .prql import Prql
from .value import ValueDataFrame


class PipelineStatement(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

        logging.debug("pipeline_statement", extra={
            "indent": self.__deep,
        })

    def run(self, tree: Tree):
        self.visit_children(tree)

    def dataframe(self, tree: Tree):
        dataframe_interpreter = ValueDataFrame(self.__deep + 1, self.__runtime)
        dataframe_interpreter.visit(tree)

    def workflow_access_let_use(self, tree: Tree):
        accessor = WorkflowAccessLetUse(self.__deep + 1, self.__runtime)
        accessor.visit(tree)

    def let_use(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline let handler")
        name = tree.children[0].value
        self.__runtime.add_stack(LetUseInstruction(name))

    def step_use(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline step handler")
        name = tree.children[0].value
        self.__runtime.add_stack(StepUseInstruction(name))

    def prql(self, tree: Tree):
        Prql(self.__deep + 1, self.__runtime).run(tree)

    def error_use(self, tree: Tree):
        logging.debug("pipeline_error_handler", extra={
            "indent": self.__deep,
        })
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid pipeline error handler")
        name = tree.children[0].value
        self.__runtime.add_stack(ErrorUseInstruction(name))
