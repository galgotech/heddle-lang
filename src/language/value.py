from typing import List, Dict
import json

from lark.visitors import Interpreter
from lark.tree import Tree


class Value(Interpreter):
    def __init__(self):
        self.__result = None

    @property
    def result(self) -> List | Dict | str | int | float | bool | None:
        return self.__result

    def value(self, tree):
        child = tree.children[0]
        if isinstance(child, Tree):
            self.visit(child)
        else:
            token_type = child.type
            if token_type == 'ESCAPED_STRING':
                self.__result = json.loads(child.value)
            elif token_type == 'SIGNED_NUMBER':
                self.__result = json.loads(child.value)
            elif token_type == 'TRUE':
                self.__result = True
            elif token_type == 'FALSE':
                self.__result = False
            elif token_type == 'NULL':
                self.__result = None

    def list(self, tree):
        list_interpreter = List()
        list_interpreter.visit_children(tree)
        self.__result = list_interpreter.list

    def dict(self, tree):
        dict_interpreter = Dict()
        dict_interpreter.visit_children(tree)
        self.__result = dict_interpreter.dict


class List(Interpreter):
    def __init__(self):
        self.__list = []

    @property
    def list(self):
        return self.__list

    def value(self, tree):
        value_interpreter = Value()
        value_interpreter.visit(tree)
        self.__list.append(value_interpreter.result)


class Dict(Interpreter):
    def __init__(self):
        self.__dict = {}

    @property
    def dict(self):
        return self.__dict

    def pair(self, tree):
        key = tree.children[0].value

        value_interpreter = Value()
        value_interpreter.visit(tree.children[1])

        self.__dict[key] = value_interpreter.result
