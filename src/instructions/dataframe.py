import polars as pl
from instructions.instruction import Instruction


class DataFrameInstruction(Instruction):
    def __init__(self, dataFrame: pl.DataFrame) -> None:
        self.__dataframe = dataFrame

    @property
    def dataframe(self) -> pl.DataFrame:
        return self.__dataframe

    def name(self) -> str:
        return "dataframe"
