from .expr import Expr
from typing import (
    TypeVar,
)

T = TypeVar("T")
V = TypeVar("V")


class Scan(Expr):
    def __init__(self, expr: Expr):
        self.expr = expr

    def exec(self):
        return self.expr.exec()


class FilteredScan(Expr):
    def __init__(self, expr, filterfn):
        self.expr = expr
        self.filter = filterfn

    def exec(self):
        return (row for row in self.expr.exec() if self.filter(row))


# TODO: seq->Expr?


class SortedIndexScan:
    def __init__(self, seq, index, key):
        self.seq = seq
        self.index = index
        self.key = key

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(key))


class RangeIndexedScan(Expr):
    def __init__(self, seq, index, start, end):
        self.seq = seq
        self.index = index
        self.start = start
        self.end = end

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(self.start, self.end))
