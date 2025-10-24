import json
import logging
from typing import Dict, List
from lark import Token
import polars as pl

from lark.visitors import Interpreter

from instructions.dataframe import DataFrameInstruction
from runtime.local import Runtime


class ValueDataFrame(Interpreter):
    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

        logging.debug("list", extra={
            "indent": self.__deep,
        })

    def dict(self, tree):
        dict_interpreter = ValueDict(self.__deep + 1)
        dict_interpreter.visit_children(tree)

        values = dict_interpreter.result
        self.__runtime.add_stack(DataFrameInstruction(pl.DataFrame(values)))


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
    def result(self) -> Dict:
        return self.__values

    def run(self, tree):
        logging.debug("ValueDict", extra={
            "indent": self.__deep,
        })
        self.visit(tree)

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
