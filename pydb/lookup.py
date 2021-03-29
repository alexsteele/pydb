from .expr import Expr
from .table import ITable
from .index import Index
from dataclasses import dataclass
from typing import Any


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
