from lark.visitors import Interpreter
from lark.tree import Tree

from .value import Value
from .let_statement import LetStatement
from .pipeline_statement import PipeLineStatement


class AnonymousScope(Interpreter):
    def __init__(self, memory, modules):
        self.__memory = memory
        self.__modules = modules
        self.__result = None

    @property
    def result(self):
        return self.__result

    def visit(self, tree):
        self.__memory.enter_scope()
        self.visit_children(tree)
        self.__memory.exit_scope()

    def let_statement(self, tree):
        interpreter = LetStatement(self.__memory, self.__modules)
        interpreter.visit(tree)
        self.__result = interpreter.result

    def pipeline_statement(self, tree):
        interpreter = PipeLineStatement(self.__memory, self.__modules)
        interpreter.visit(tree)
        self.__result = interpreter.result

    def anonymous_scope(self, tree):
        scope = AnonymousScope(self.__memory, self.__modules)
        scope.visit(tree)
        self.__result = scope.result

    def scope_return(self, tree):
        return_node = tree.children[0]
        if isinstance(return_node, Tree):
            if return_node.data == 'value':
                value_interpreter = Value()
                value_interpreter.visit(return_node)
                self.__result = value_interpreter.result
            elif return_node.data == 'pipeline_statement':
                pipeline_interpreter = PipeLineStatement(self.__memory, self.__modules)
                pipeline_interpreter.visit(return_node)
                self.__result = pipeline_interpreter.result
        else:
            if return_node.type == 'VARIABLE_NAME':
                self.__result = self.__memory.get(return_node.value)