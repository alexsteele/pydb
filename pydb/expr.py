from dataclasses import dataclass
from typing import Iterator, Tuple

from .table import ITable


class Expr:
    def exec(self) -> Iterator[Tuple]:
        pass


@dataclass
class Rows(Expr):
    table: ITable

    def exec(self) -> Iterator[Tuple]:
        return self.table.rows()
