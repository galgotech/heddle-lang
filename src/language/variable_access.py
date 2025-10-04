from lark.visitors import Interpreter
from .memory import Memory


class VariableAccess(Interpreter):
    __memory: Memory
    __result: any

    def __init__(self, memory: Memory):
        self.__memory = memory
        self.__result = None

    @property
    def result(self):
        return self.__result

    def variable_access(self, tree):
        workflow_name = tree.children[0].value
        variable_name = tree.children[1].value

        try:
            workflow_scope = self.__memory.get(workflow_name)
        except NameError:
            raise NameError(f"Workflow '{workflow_name}' is not defined")

        if not isinstance(workflow_scope, dict):
            raise TypeError(f"Workflow '{workflow_name}' is not a valid scope.")

        try:
            self.__result = workflow_scope[variable_name]
        except KeyError:
            raise NameError(
                f"Variable '{variable_name}' is not defined in workflow '{workflow_name}'"
            )
