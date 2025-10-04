from typing import Dict
from lark import Token
from lark.visitors import Interpreter

from .anonymous_scope import AnonymousScope
from .let_statement import LetStatement
from .memory import Memory
from .pipeline_statement import PipelineStatement
from .value import Value


class WorkflowDefinition(Interpreter):
    __modules: Dict
    __memory: Memory
    __name: str
    __return_value: any
    __return_value_is_set: bool

    def __init__(self, memory: Memory, modules: Dict):
        self.__memory = memory
        self.__modules = modules
        self.__name = None
        self.__return_value = None
        self.__return_value_is_set = False

    def visit(self, tree):
        self.__name = tree.children[0].value

        self.__memory.enter_scope()
        self.visit_children(tree)

        value_to_set = (
            self.__return_value
            if self.__return_value_is_set
            else self.__memory.current_scope
        )

        self.__memory.exit_scope()
        self.__memory.set(self.__name, value_to_set)

    def let_statement(self, tree):
        let_statement = LetStatement(self.__memory, self.__modules)
        let_statement.visit(tree)

    def pipeline_statement(self, tree):
        interpreter = PipelineStatement(self.__memory, self.__modules)
        interpreter.visit(tree)

    def anonymous_scope(self, tree):
        interpreter = AnonymousScope(self.__memory, self.__modules)
        interpreter.run(tree)

    def scope_return(self, tree):
        self.__return_value_is_set = True
        return_node = tree.children[0]

        if isinstance(return_node, Token) and return_node.type == "VARIABLE_NAME":
            self.__return_value = self.__memory.get(return_node.value)
        elif return_node.data == "value":
            value_interpreter = Value()
            value_interpreter.visit(return_node)
            self.__return_value = value_interpreter.result
        elif return_node.data == "pipeline_statement":
            self.__return_value = PipelineStatement(
                self.__memory, self.__modules
            ).visit(return_node)