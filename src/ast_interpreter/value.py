import json
import logging
from typing import Dict, List
from lark import Token
import polars as pl

from lark.visitors import Interpreter


class ValueDataFrame(Interpreter):
    __dataframe: pl.DataFrame

    def __init__(self, deep: int):
        self.__deep = deep
        self.__dataframe = pl.DataFrame()

        logging.debug("list", extra={
            "indent": self.__deep,
        })

    @property
    def result(self) -> pl.DataFrame:
        return self.__dataframe

    def dict(self, tree):
        dict_interpreter = ValueDict(self.__deep + 1)
        dict_interpreter.visit_children(tree)

        values = dict_interpreter.result
        self.__dataframe = pl.DataFrame(values)


class ValueDict(Interpreter):
    __deep: int
    __values: Dict[str, List]
    __column: str | None

    def __init__(self, deep: int):
        self.__deep = deep
        self.__values = {}
        self.__column = None

        logging.debug("dict", extra={
            "indent": self.__deep,
        })

    @property
    def result(self):
        return self.__values

    def visit(self, tree):
        logging.debug("ValueDict", extra={
            "indent": self.__deep,
        })
        super().visit(tree)

    def pair(self, tree):
        self.__column = tree.children[0].value
        if self.__column is None:
            raise Exception("invalid pair")

        self.__values[self.__column] = []
        logging.debug("pair: %s", {"column": self.__column}, extra={
            "indent": self.__deep,
        })
        self.visit_children(tree)

    def primitive(self, tree):
        primitive_interpreter = ValuePrimitive(self.__deep + 1)
        primitive_interpreter.visit(tree)

        if self.__column is None:
            raise Exception("invalid column name")

        self.__values[self.__column].append(primitive_interpreter.result)
        self.__column = None


class ValuePrimitive(Interpreter):
    __value: int | float | str | bool | None

    def __init__(self, deep: int):
        self.__deep = deep
        self.__value = None

        logging.debug("list", extra={
            "indent": self.__deep,
        })

    @property
    def result(self) -> int | float | str | bool | None:
        return self.__value

    def visit(self, tree):
        child = tree.children[0]
        if not isinstance(child, Token):
            raise Exception("invalid primitive")

        token_type = child.type
        if token_type == 'ESCAPED_STRING':
            self.__value = json.loads(child.value)
        elif token_type == 'SIGNED_NUMBER':
            self.__value = json.loads(child.value)
        elif token_type == 'TRUE':
            self.__value = True
        elif token_type == 'FALSE':
            self.__value = False
        elif token_type == 'NULL':
            self.__value = None
