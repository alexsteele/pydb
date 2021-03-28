from typing import (
    Any,
    Iterator,
    Tuple,
    Sequence,
    Dict,
)
from enum import Enum
from dataclasses import dataclass


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


@dataclass
class Schema:
    name: str
    columns: Sequence[Column]

    def columnid(self, name):
        return next(i for i, c in enumerate(self.columns) if c.name == name)

    def columnids(self, *names):
        return map(self.columnid, names) if names else range(len(self.columns))

    def column_names(self):
        return tuple(c.name for c in self.columns)


# TODO: remove?
@dataclass
class Table:
    def __init__(self, name, *columns, **options):
        self.name = name
        self.columns = columns
        self.options = options


class ITable:
    def name(self) -> str:
        return self.schema().name

    def schema(self) -> Schema:
        pass

    def rows(self) -> Iterator[Tuple]:
        pass
