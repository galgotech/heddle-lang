import logging
from lark import Tree
from lark.visitors import Interpreter
from runtime.local import Runtime


class LetUse(Interpreter):
    __deep: int
    __runtime: Runtime

    def __init__(self, deep: int, runtime: Runtime):
        super().__init__()
        self.__deep = deep
        self.__runtime = runtime

    def run(self, tree: Tree) -> None:
        logging.debug("let_use", extra={
            "indent": self.__deep,
        })
        self.visit_children(tree)
