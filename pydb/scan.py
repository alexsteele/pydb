import itertools
from .index import Index
from typing import (
    TypeVar,
    Iterator,
    Iterable,
    Optional,
)

T = TypeVar("T")
V = TypeVar("V")


class Scan:
    def __init__(self, seq):
        self.seq = seq

    def exec(self) -> Iterator[V]:
        return iter(self.seq)


class FilteredScan:
    def __init__(self, expr, filterfn):
        self.expr = expr
        self.filter = filterfn

    def exec(self):
        return (row for row in self.expr.eval() if self.filter(row))


# TODO: seq->Expr?


class SortedIndexScan:
    def __init__(self, seq, index, key):
        self.seq = seq
        self.index = index
        self.key = key

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(key))


class RangeIndexedScan:
    def __init__(self, seq, index, start, end):
        self.seq = seq
        self.index = index
        self.start = start
        self.end = end

    def exec(self):
        return (self.seq[rid] for rid in self.index.scan(self.start, self.end))
