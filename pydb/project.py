from .expr import Expr
from typing import TypeVar

A = TypeVar("A")
B = TypeVar("B")


class DynamicProjection(Expr):
    def __init__(self, seq, fn):
        self.seq = seq
        self.fn = fn

    def exec(self):
        return (self.fn(row) for row in self.seq)


class ColumnProjection(Expr):
    def __init__(self, expr, columns):
        self.expr = expr
        self.columns = columns

    def exec(self):
        return (self._project(row) for row in self.expr.exec())

    def _project(self, row):
        return tuple(row[col] for col in self.columns)
