from abc import ABC, abstractmethod
import pyarrow as pa
from typing import Any

class HeddleTable(ABC):
    """
    HeddleTable is the universal interface for data exchange in Heddle Lang.
    All polyglot steps must receive and return an implementation of this interface.
    """
    @property
    @abstractmethod
    def native(self) -> pa.Table:
        """Returns the underlying PyArrow Table."""
        pass

    @property
    @abstractmethod
    def num_rows(self) -> int:
        pass

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        pass

    @abstractmethod
    def to_pandas(self):
        pass

    @abstractmethod
    def to_pydict(self) -> dict:
        pass

    @abstractmethod
    def to_bytes(self) -> bytes:
        pass

class Table(HeddleTable):
    """
    Concrete implementation of HeddleTable wrapping a PyArrow Table.
    """
    def __init__(self, data: pa.Table):
        self._data = data

    @property
    def native(self) -> pa.Table:
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

    def to_bytes(self) -> bytes:
        """Serializes the table to Arrow IPC stream format."""
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, self._data.schema) as writer:
            writer.write_table(self._data)
        return sink.getvalue().to_pybytes()
