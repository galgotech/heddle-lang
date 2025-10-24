from ast import Dict
import logging
from lark import Token, Tree
from lark.visitors import Interpreter

from ast_interpreter.value import ValueDict
from instructions.error import ErrorInstruction
from runtime.local import Runtime


class ErrorStatement(Interpreter):
    __deep: int
    __runtime: Runtime
    __config: Dict

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid error name")
        
        name = nameChild.value
        logging.debug("error: %s", {"name": name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)
        self.__runtime.add_stack(ErrorInstruction(name, self.__config))

    def schema_type_dict(self, tree: Tree):
        interpreter = ValueDict(self.__deep + 1)
        interpreter.run(tree)
        self.__config = interpreter.result
