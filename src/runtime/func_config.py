

from typing import Dict


class FuncConfig:
    __call: str
    __call_config: Dict

    def __init__(self, call: str, call_config: Dict) -> None:
        self.__call = call
        self.__call_config = call_config

    @property
    def call(self) -> str:
        return self.__call

    @property
    def call_config(self) -> Dict:
        return self.__call_config
