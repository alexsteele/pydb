from dataclasses import dataclass
from typing import Any

from .expr import Expr
from .index import Index
from .table import ITable


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
