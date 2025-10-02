from typing import Dict, List
from lark.visitors import Interpreter

from language.memory import Memory
from .value import Value
from .pipeline_statement import PipeLineStatement

class LetStatement(Interpreter):
    __modules: Dict
    __memory: Memory
    __name: str
    __value: List | Dict | str | int | float | bool | None

    def __init__(self, memory, modules):
        self.__memory = memory
        self.__modules = modules
        self.__name = None
        self.__value = None

    @property
    def result(self):
        return self.__value

    def visit(self, tree):
        assert len(tree.children) == 2
        assert tree.children[0].type == "VARIABLE_NAME"
        self.__name = tree.children[0].value
    
        self.visit_children(tree)

        self.__memory.set(self.__name, self.__value)

    def let_expression(self, tree):
        expression_node = tree.children[0]

        if expression_node.data == 'value':
            value_interpreter = Value()
            value_interpreter.visit(expression_node)
            self.__value = value_interpreter.result

        elif expression_node.data == 'pipeline_statement':
            pipeline_interpreter = PipeLineStatement(self.__memory, self.__modules)
            pipeline_interpreter.visit(expression_node)
            self.__value = pipeline_interpreter.result
