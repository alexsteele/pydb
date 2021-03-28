from dataclasses import dataclass
from .table import ITable
from typing import (
    Iterator,
    Tuple,
)


class Expr:
    def exec(self) -> Iterator[Tuple]:
        pass


@dataclass
class Rows(Expr):
    table: ITable

    def exec(self) -> Iterator[Tuple]:
        return self.table.rows()
