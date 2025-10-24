import logging
from typing import Dict
from lark import Token, Tree
from lark.visitors import Interpreter

from ast_interpreter.value import ValueDict
from instructions.schema import SchemaInstruction
from runtime.local import Runtime


class SchemaStatement(Interpreter):
    __deep: int
    __runtime: Runtime
    __schema: Dict

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree):
        nameChild = tree.children[0]
        if not isinstance(nameChild, Token):
            raise Exception("invalid schema name")

        name = nameChild.value
        logging.debug("schema: %s", {"name": name}, extra={
            "indent": self.__deep,
        })

        self.visit_children(tree)
        self.__runtime.add_stack(SchemaInstruction(name, self.__schema))

    def schema_type_dict(self, tree: Tree):
        interpreter = ValueDict(self.__deep + 1)
        interpreter.run(tree)
        self.__schema = interpreter.result
