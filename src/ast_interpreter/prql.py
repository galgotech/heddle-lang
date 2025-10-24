import logging
from lark import Token, Tree
from instructions.prql import PrqlInstruction
from runtime.local import Runtime


class Prql:
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid prql")

        prql_query = tree.children[0].value.strip()

        logging.debug("prql: %s", {"query": prql_query}, extra={
            "indent": self.__deep,
        })
        self.__runtime.add_stack(PrqlInstruction(prql_query))
