import pyarrow as pa
from pydantic import BaseModel, ConfigDict
from typing import Any

class Table:
    """
    Table is a high-level wrapper around a PyArrow Table, providing 
    zero-copy data access and integration with Heddle's locality resolution.
    """
    def __init__(self, data: pa.Table):
        self._data = data

    @property
    def native(self) -> pa.Table:
        """Returns the underlying PyArrow Table."""
        return self._data

    @property
    def num_rows(self) -> int:
        return self._data.num_rows

    @property
    def schema(self) -> pa.Schema:
        return self._data.schema

    def to_pandas(self):
        """Zero-copy conversion to a Pandas DataFrame (if applicable)."""
        return self._data.to_pandas()

    def to_pydict(self) -> dict:
        return self._data.to_pydict()
