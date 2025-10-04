from typing import Dict
from lark import Token, Tree
from lark.visitors import Interpreter

from language.variable_access import VariableAccess
from .memory import Memory
from .prql import Prql
from .value import Value


class PipelineStatement(Interpreter):
    __memory: Memory
    __modules: Dict
    __value: any

    def __init__(self, memory: Memory, modules: Dict):
        self.__memory = memory
        self.__modules = modules
        self.__value = None

    @property
    def result(self):
        return self.__value

    def visit(self, tree: Tree):
        self.__value = None
        children = tree.children
        first_child = children[0]
        children_to_process = children

        is_initial_value_node = False
        if isinstance(first_child, Token) and first_child.type == 'VARIABLE_NAME':
            is_initial_value_node = True
        elif isinstance(first_child, Tree) and first_child.data in ('value', 'variable_access'):
            is_initial_value_node = True

        if is_initial_value_node:
            if isinstance(first_child, Token):
                self.__value = self.__memory.get(first_child.value)
            else:
                self._visit_tree(first_child)
            children_to_process = children[1:]

        for child in children_to_process:
            self._visit_tree(child)

        return self.result

    def value(self, tree: Tree):
        value_interpreter = Value()
        value_interpreter.visit(tree)
        self.__value = value_interpreter.result

    def variable_access(self, tree: Tree):
        accessor = VariableAccess(self.__memory)
        accessor.visit(tree)
        self.__value = accessor.result

    def pipeline_function_handler(self, tree: Tree):
        module_name = tree.children[0].value
        function_name = tree.children[1].value

        module = self.__modules[module_name]
        function = module[function_name]

        if self.__value is not None:
            self.__value = function(self.__value)

    def prql(self, tree: Tree):
        prql_interpreter = Prql(self.__memory, self.__value)
        prql_interpreter.visit(tree)
        self.__value = prql_interpreter.to_polars()
