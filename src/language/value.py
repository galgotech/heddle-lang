import logging
import json
from typing import Dict, List

from lark.visitors import Interpreter
from lark.tree import Tree


class Value(Interpreter):
    __deep: int
    __result: List | Dict | str | int | float | bool | None

    def __init__(self, deep: int):
        self.__deep = deep
        self.__result = None

    @property
    def result(self) -> List | Dict | str | int | float | bool | None:
        return self.__result

    def value(self, tree):
        child = tree.children[0]
        if isinstance(child, Tree):
            logging.debug("value", extra={
                "indent": self.__deep,
            })
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

            logging.debug("value: %s", self.__result, extra={
                "indent": self.__deep,
            })

    def list(self, tree):
        list_interpreter = ValueList(self.__deep + 1)
        list_interpreter.visit_children(tree)
        self.__result = list_interpreter.list

    def dict(self, tree):
        dict_interpreter = ValueDict(self.__deep + 1)
        dict_interpreter.visit_children(tree)
        self.__result = dict_interpreter.dict


class ValueList(Interpreter):
    __list: List

    def __init__(self, deep: int):
        self.__deep = deep
        self.__list = []

        logging.debug("list", extra={
            "indent": self.__deep,
        })

    @property
    def list(self) -> List:
        return self.__list

    def value(self, tree):
        value_interpreter = Value(self.__deep + 1)
        value_interpreter.visit(tree)
        self.__list.append(value_interpreter.result)


class ValueDict(Interpreter):
    __deep: int
    __key: str | None = None

    def __init__(self, deep: int):
        self.__deep = deep
        self.__dict = {}
        self.__key = None

        logging.debug("dict", extra={
            "indent": self.__deep,
        })

    @property
    def dict(self):
        return self.__dict

    def visit(self, tree):
        logging.debug("visit", extra={
            "indent": self.__deep,
        })
        super().visit(tree)

    def pair(self, tree):
        self.__key = tree.children[0].value
        logging.debug("pair: %s", {"key": self.__key}, extra={
            "indent": self.__deep,
        })
        self.visit_children(tree)

    def value(self, tree):
        value_interpreter = Value(self.__deep + 1)
        value_interpreter.visit(tree)

        self.__dict[self.__key] = value_interpreter.result
        self.__key = None
