from dataclasses import dataclass
from typing import Any, Callable, Sequence, Tuple

from .expr import Expr


@dataclass
class DynamicProjection(Expr):
    expr: Expr
    fn: Callable[[Tuple], Any]

    def exec(self):
        return (self.fn(row) for row in self.seq)


@dataclass
class ColumnProjection(Expr):
    expr: Expr
    columns: Sequence[int]

    def exec(self):
        return (self._project(row) for row in self.expr.exec())

    def _project(self, row):
        return tuple(row[col] for col in self.columns)
