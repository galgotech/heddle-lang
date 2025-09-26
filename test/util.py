from typing import List
from lark import Tree


def search_children(ast: Tree, type: str) -> List[Tree]:
    if ast.data == type:
        return [ast]

    trees: List[Tree] = []
    for child in ast.children:
        if isinstance(child, Tree):
            # print(child.data)
            # print(child)
            trees += search_children(child, type)

    return trees
