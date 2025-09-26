from lark.visitors import Interpreter

class VariableAccess(Interpreter):
    def __init__(self, memory):
        self.__memory = memory
        self.__result = None

    @property
    def result(self):
        return self.__result

    def visit(self, tree):
        workflow_name = tree.children[0].value
        variable_name = tree.children[1].value

        try:
            workflow_scope = self.__memory.get(workflow_name)
        except NameError:
            raise NameError(f"Workflow '{workflow_name}' is not defined") from None

        workflow_scope = self.__memory.get(workflow_name)
        if workflow_scope is None:
            raise NameError(f"Workflow '{workflow_name}' is not defined")

        if variable_name not in workflow_scope:
            raise NameError(f"Variable '{variable_name}' is not defined in workflow '{workflow_name}'")

        self.__result = workflow_scope[variable_name]