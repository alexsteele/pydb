from typing import (
    Iterator,
    Tuple,
    Sequence,
)
from enum import Enum
from dataclasses import dataclass, field


class DataType(Enum):
    INT = "INT"
    STRING = "STRING"


class ColumnAttr(Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    AUTO_INCREMENT = "AUTO_INCREMENT"
    NOT_NULL = "NOT_NULL"


@dataclass
class Column:
    name: str
    dtype: DataType
    attrs: Sequence[ColumnAttr] = field(default_factory=list)


@dataclass
class Schema:
    name: str
    columns: Sequence[Column]

    def columnid(self, name):
        return next(i for i, c in enumerate(self.columns) if c.name == name)

    def columnids(self, *names):
        return map(self.columnid, names) if names else range(len(self.columns))


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
