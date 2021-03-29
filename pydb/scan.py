from dataclasses import dataclass
from .expr import Expr
from .index import SortedIndex, RangeIndex
from .table import ITable
from typing import (
    TypeVar,
    Callable,
    Any,
    Tuple,
    Sequence,
)

T = TypeVar("T")
V = TypeVar("V")


@dataclass
class Scan(Expr):
    expr: Expr

    def exec(self):
        return self.expr.exec()


@dataclass
class FilteredScan(Expr):
    expr: Expr
    filter: Callable[[Tuple], bool]

    def exec(self):
        return (row for row in self.expr.exec() if self.filter(row))




# TODO: seq->Expr?
@dataclass
class SortedIndexScan:
    seq: Sequence[Tuple]
    index: SortedIndex
    key: Any

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(key))


@dataclass
class RangeIndexedScan(Expr):
    seq: Sequence[Tuple]
    index: RangeIndex
    start: Any
    end: Any

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(self.start, self.end))
