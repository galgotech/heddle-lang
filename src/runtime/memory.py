from typing import Dict
import polars as pl


class ImmutableVariable(Exception):
    pass


class Memory:
    __current_scope: str | None
    __state: Dict[str, Dict[str, pl.DataFrame]]

    def __init__(self):
        self.__state = {}
        self.__current_scope = None

    def get(self, name: str, scope: str | None = None) -> pl.DataFrame:
        _scope = scope if scope is not None else self.__current_scope
        if _scope is None:
            raise Exception("no current scope")
        return self.__state[_scope][name]

    def has(self, name: str, scope: str | None = None) -> bool:
        try:
            self.get(name, scope)
            return True
        except KeyError:
            return False

    def set(self, name: str, value: pl.DataFrame):
        if self.__current_scope is None:
            raise Exception("no current scope")
        try:
            self.__state[self.__current_scope][name]
            raise ImmutableVariable(name)
        except KeyError:
            self.__state[self.__current_scope][name] = value

    def enter_scope(self, name: str):
        try:
            self.__state[name]
        except KeyError:
            self.__state[name] = {}
        self.__current_scope = name
