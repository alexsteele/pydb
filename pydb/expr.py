from collections import defaultdict
from enum import Enum
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Iterator,
    Tuple,
    Protocol,
    Sequence,
)

from .table import ITable
from .index import Index
from .table import ITable


class Expr:
    def exec(self) -> Iterator[Tuple]:
        pass


@dataclass
class Scan(Expr):
    table: ITable

    def exec(self) -> Iterator[Tuple]:
        return self.table.rows()


@dataclass
class IndexedLookup(Expr):
    table: ITable
    index: Index[Any, int]
    key: Any

    def exec(self):
        rowid = self.index.find(self.key)
        if not rowid:
            return ()
        return (self.table.get(rowid),)


class Condition(Protocol):
    def test(self, row: Tuple) -> bool:
        pass


class BinOp(Enum):
    EQ = lambda x, y: x == y
    LT = lambda x, y: x < y
    GT = lambda x, y: x > y


@dataclass
class ValueComp(Condition):
    op: BinOp
    col: int
    val: Any

    def test(self, row):
        return self.op(row[self.col], self.val)


@dataclass
class ColumnComp(Condition):
    op: BinOp
    col1: int
    col2: int

    def test(self, row):
        return self.op(row[self.col1], row[self.col2])


@dataclass
class FilteredScan(Expr):
    table: ITable
    cond: Condition

    def exec(self):
        return (row for row in self.table.rows() if self.cond.test(row))


@dataclass
class IndexedJoin(Expr):
    expr: Expr
    column: int
    index: Index[Any, int]
    table: ITable

    def exec(self):
        for row1 in self.expr.exec():
            rowid = self.index.find(row1[self.column])
            if rowid:
                row2 = self.table.get(rowid)
                assert row2 is not None
                yield (*row1, *row2)


@dataclass
class HashJoin(Expr):
    exp1: Expr
    exp2: Expr
    col1: int
    col2: int

    def exec(self):
        index = defaultdict(list)
        for row1 in self.exp1.exec():
            index[row1[self.col1]].append(row1)
        for row2 in self.exp2.exec():
            for row1 in index[row2[self.col2]]:
                yield (*row1, *row2)


class MergeJoin(Expr):
    def __init__(self, exp1, keyfn1, exp2, keyfn2):
        self.exp1
        self.keyfn1 = keyfn1
        self.exp2 = exp2
        self.keyfn2 = keyfn2

    def exec(self):
        sentinel = object()
        it1 = self.exp1.exec()
        it2 = self.exp2.exec()
        a = next(it1, sentinel)
        b = next(it2, sentinel)
        while sentinel not in (a, b):
            k1 = self.keyfn1(a)
            k2 = self.keyfn2(b)
            if k1 == k2:
                yield (a, b)
                a = next(it1, sentinel)
                b = next(it2, sentinel)
            elif k1 < k2:
                a = next(it1, sentinel)
            else:
                b = next(it2, sentinel)


@dataclass
class ColumnProjection(Expr):
    expr: Expr
    columns: Sequence[int]

    def exec(self):
        return (self._project(row) for row in self.expr.exec())

    def _project(self, row):
        return tuple(row[col] for col in self.columns)
