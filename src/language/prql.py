import logging
from lark import Token, Tree
import polars as pl
import pyprql.polars_namespace  # noqa: F401


class Prql:
    __deep: int
    __result: pl.DataFrame

    def __init__(self, deep: int, data: pl.DataFrame):
        self.__deep = deep
        self.__result = data

    @property
    def result(self):
        return self.__result

    def visit(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid prql")

        prql_query = tree.children[0].value.strip()

        logging.debug("prql: %s", {"query": prql_query}, extra={
            "indent": self.__deep,
        })
        self.__result = self.__result.prql.query(prql_query)
