from lark.visitors import Interpreter
from lark.tree import Tree

from .value import Value
from .let_statement import LetStatement
from .pipeline_statement import PipeLineStatement


class AnonymousScope(Interpreter):
    def __init__(self, memory, modules):
        self.__memory = memory
        self.__modules = modules
        self.result = None

    def run(self, tree):
        # tree is an 'anonymous_scope' node. Its child is a 'scope' node.
        scope_node = tree.children[0]
        self.__memory.enter_scope()

        # Manually visit the children of the scope node.
        for child in scope_node.children:
            self.visit(child)

        self.__memory.exit_scope()
        return self.result

    def let_statement(self, tree):
        interpreter = LetStatement(self.__memory, self.__modules)
        interpreter.visit(tree)
        self.result = interpreter.result

    def pipeline_statement(self, tree):
        interpreter = PipeLineStatement(self.__memory, self.__modules)
        interpreter.visit(tree)
        self.result = interpreter.result

    def anonymous_scope(self, tree): # for nested scopes
        interpreter = AnonymousScope(self.__memory, self.__modules)
        self.result = interpreter.run(tree)

    def scope_statement(self, tree):
        # A scope_statement is a wrapper. Visit its child.
        self.visit(tree.children[0])

    def scope_return(self, tree):
        return_node = tree.children[0]
        if isinstance(return_node, Tree):
            if return_node.data == 'value':
                value_interpreter = Value()
                value_interpreter.visit(return_node)
                self.result = value_interpreter.result
            elif return_node.data == 'pipeline_statement':
                pipeline_interpreter = PipeLineStatement(self.__memory, self.__modules)
                pipeline_interpreter.visit(return_node)
                self.result = pipeline_interpreter.result
        else:
            if return_node.type == 'VARIABLE_NAME':
                self.result = self.__memory.get(return_node.value)