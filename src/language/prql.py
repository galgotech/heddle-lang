import json
import logging
from typing import Dict, List
from lark import Token, Tree
import prqlc


class Prql:
    __deep: int
    __result: Dict | List | str | int | float | bool | None

    def __init__(self, deep: int, data: Dict | List | str | int | float | bool | None):
        self.__deep = deep
        self.__result = data

    @property
    def result(self):
        return self.__result

    def visit(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid prql")

        prql_query = tree.children[0].value.strip()

        print(prqlc.prql_to_pl(prql_query))
        # print(prqlc.get_targets())
        # result = prqlc.pl_to_rq(prqlc.prql_to_pl(prql_query))
        # print(json.dumps(json.loads(result), indent=2))

        # options = prqlc.CompileOptions(
        #     format=True, signature_comment=True, target="sql.postgres"
        # )
        # sql = prqlc.compile(prql_query)
        # sql_postgres = prqlc.compile(prql_query, options)
        # print(sql)
        # print(sql_postgres)

        logging.debug("prql: %s", {"query": prql_query}, extra={
            "indent": self.__deep,
        })
        self.__result = {}
