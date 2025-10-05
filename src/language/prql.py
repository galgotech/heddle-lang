import re
import polars as pl
import prql_python as prql
from lark import Token, Tree
from .memory import Memory


class Prql:
    __deep: int
    __memory: Memory
    __data_frame_in: pl.DataFrame | None
    __data_frame_for_query: pl.DataFrame
    __prql_to_compile: str

    def __init__(self, deep: int, memory: Memory, data_frame: pl.DataFrame | None = None):
        self.__deep = deep
        self.__memory = memory
        self.__data_frame_in = data_frame

    def visit(self, tree: Tree):
        if not isinstance(tree.children[0], Token):
            raise Exception("invalid prql")

        raw_query = tree.children[0].value.strip()

        if self.__data_frame_in is not None:
            # Data is piped in
            if re.search(r"^from\s+", raw_query):
                raise ValueError("PRQL query with piped-in data cannot have a 'from' clause.")
            self.__data_frame_for_query = self.__data_frame_in
            self.__prql_to_compile = f"from df\n{raw_query}"
        else:
            # Data is not piped in, so it must be specified in the query
            match = re.search(r"from\s+([a-zA-Z_][a-zA-Z0-9_]*)", raw_query)
            if not match:
                raise ValueError("PRQL query must have a 'from' clause when no data is piped in.")

            table_name = match.group(1)
            self.__data_frame_for_query = self.__memory.get(table_name)

            # Replace the original table name with `df` so the compiled SQL is consistent
            self.__prql_to_compile = re.sub(r"from\s+[a-zA-Z_][a-zA-Z0-9_]*", "from df", raw_query, 1)

    def to_polars(self) -> pl.DataFrame:
        sql_query = prql.compile(self.__prql_to_compile)

        # We need to register the dataframe with the name `df` in the SQL context.
        context = pl.SQLContext()
        context.register("df", self.__data_frame_for_query)
        return context.execute(sql_query, eager=True)
