import logging
from lark.visitors import Interpreter

from instructions.workflow_access_let_use import WorkflowAccessLetUseInstruction
from runtime.local import Runtime


class WorkflowAccessLetUse(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def variable_access(self, tree) -> None:
        workflow = tree.children[0].value
        variable = tree.children[1].value

        logging.debug("variable_access: %s", {"workflow": workflow, "variable": variable}, extra={
            "indent": self.__deep,
        })

        self.__runtime.add_stack(WorkflowAccessLetUseInstruction(workflow, variable))
