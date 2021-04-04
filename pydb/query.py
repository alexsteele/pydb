from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Sequence, Union

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
class On:
    expr: QueryExpr


class JoinKind(Enum):
    INNER = "INNER"
    LEFT_OUTER = "LEFT_OUTER"
    RIGHT_OUTER = "RIGHT_OUTER"
    FULL_OUTER = "FULL_OUTER"


@dataclass
class Join:
    tables: Sequence[str]
    condition: Optional[On] = None
    kind: JoinKind = JoinKind.INNER


@dataclass
class From:
    table: Union[str, Join]


@dataclass
class Where:
    condition: QueryExpr


@dataclass
class Select(Query):
    exprs: Sequence[str]  # TODO: support select exprs
    from_clause: From
    where_clause: Optional[Where] = None
