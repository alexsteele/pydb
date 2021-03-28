from .table import Schema
from dataclasses import dataclass
from typing import (
    Any,
    Sequence,
    Optional,
)


class Query:
    pass


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


class WhereExpr:
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
class BinExpr(WhereExpr):
    op: str
    left: WhereExpr
    right: WhereExpr


@dataclass
class Where:
    condition: WhereExpr


@dataclass
class Select(Query):
    exprs: Sequence[str]  # TODO: support select exprs
    from_clause: From
    where_clause: Optional[Where] = None
