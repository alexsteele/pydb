from dataclasses import dataclass
from typing import Any, Optional, Sequence

from .table import Schema


class Query:
    pass


class QueryExpr:
    pass


class Operand:
    pass


@dataclass
class Symbol(Operand):
    val: str


@dataclass
class Const(Operand):
    val: Any


@dataclass
class BinExpr(QueryExpr):
    op: str
    left: Operand
    right: Operand


@dataclass
class CreateTable(Query):
    schema: Schema


@dataclass
class Insert(Query):
    table: str
    columns: Sequence[str]
    values: Sequence[Any]


@dataclass
class From:
    table: str


@dataclass
class Where:
    condition: QueryExpr


@dataclass
class Select(Query):
    exprs: Sequence[str]  # TODO: support select exprs
    from_clause: From
    where_clause: Optional[Where] = None
