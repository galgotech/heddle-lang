from lark.visitors import Interpreter
from lark.tree import Tree
from .value import Value
from .variable_access import VariableAccess


class PipeLineStatement(Interpreter):
    def __init__(self, memory, modules):
        self.__memory = memory
        self.__modules = modules
        self.__result = None

    @property
    def result(self):
        return self.__result

    def visit(self, tree):
        initial_value_provider_node = tree.children[0]
        if isinstance(initial_value_provider_node, Tree):
            self.visit_children(tree)
        else:
            self.VARIABLE_NAME(initial_value_provider_node)

        for function_node in tree.children[1:]:
            self.pipeline_function_handler(function_node)

    def value(self, tree):
        value_interpreter = Value()
        value_interpreter.visit(tree)
        self.__result = value_interpreter.result

    def variable_access(self, tree):
        variable_access_interpreter = VariableAccess(self.__memory)
        variable_access_interpreter.visit(tree)
        self.__result = variable_access_interpreter.result

    def VARIABLE_NAME(self, token):
        self.__result = self.__memory.get(token.value)

    def pipeline_function_handler(self, tree):
        alias = tree.children[0].value
        function_name = tree.children[1].value

        if alias not in self.__modules:
            raise NameError(f"Module '{alias}' is not defined")

        module = self.__modules[alias]
        if function_name not in module:
            raise NameError(f"Function '{function_name}' is not defined in module '{alias}'")

        func = module[function_name]
        self.__result = func(self.__result)

    def prql(self, tree):
        # Placeholder for prql handling
        pass

    def pipeline_error_handler(self, tree):
        # Placeholder for error handling
        pass