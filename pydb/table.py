from typing import (
    Any,
    Iterator,
    Tuple,
    Sequence,
    Dict,
    Optional,
)
from enum import Enum
from dataclasses import dataclass
from .index import Index


class DataType(Enum):
    INT = "INT"
    STRING = "STRING"


class ColumnAttr(Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    AUTO_INCREMENT = "AUTO_INCREMENT"
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    DEFAULT = "DEFAULT"


@dataclass(init=False)
class Column:
    name: str
    dtype: DataType
    attrs: Dict[ColumnAttr, Any]

    def __init__(self, name, dtype, *attrs, kwattrs={}):
        self.name = name
        self.dtype = dtype
        self.attrs = {
            **{attr: True for attr in attrs},
            **kwattrs,
        }

    def hasattr(self, attr):
        return attr in self.attrs


@dataclass(init=False)
class Schema:
    name: str
    columns: Sequence[Column]

    def __init__(self, name, *columns):
        self.name = name
        self.columns = columns

    def columnid(self, name):
        return next(i for i, c in enumerate(self.columns) if c.name == name)

    def columnids(self, *names) -> Sequence[int]:
        return tuple(map(self.columnid, names) if names else range(len(self.columns)))

    def column_names(self):
        return tuple(c.name for c in self.columns)


class ITable:
    def name(self) -> str:
        return self.schema().name

    def schema(self) -> Schema:
        pass

    def insert(self, row: Tuple) -> Tuple[int, Tuple]:
        pass

    def get(self, rowid: int) -> Optional[Tuple]:
        pass

    def rows(self) -> Iterator[Tuple]:
        pass

    def indexes(self, column: Optional[str] = None) -> Sequence[Index]:
        pass
