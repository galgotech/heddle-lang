from typing import Dict, List


class Memory:
    def __init__(self):
        self.__stack = [{}]

    def get(self, name):
        for scope in reversed(self.__stack):
            if name in scope:
                return scope[name]
        raise NameError(f"Name '{name}' is not defined")

    def has(self, name: str) -> bool:
        try:
            self.get(name)
            return True
        except NameError:
            return False

    def set(self, name: str, value: Dict | List | str | int | float | bool | None):
        self.__stack[-1][name] = value

    def set_in_parent(self, name: str, value):
        if len(self.__stack) > 1:
            self.__stack[-2][name] = value
        else:
            self.__stack[-1][name] = value

    def enter_scope(self):
        self.__stack.append({})

    def exit_scope(self):
        self.__stack.pop()

    @property
    def current_scope(self):
        return self.__stack[-1]
