from .query import (
    Query,
    Select,
    Where,
    WhereExpr,
    Operand,
    BinExpr,
    Symbol,
    Const,
    Insert,
)
from .expr import Expr, Seq
from .project import ColumnProjection
from .scan import Scan, FilteredScan
from .table import Schema, ITable
from typing import (
    Dict,
)

class Planner:
    def plan(self, query: Query) -> Expr:
        pass


class SimplePlanner:
    BINOPS = {
        "=": lambda a, b: a == b,
        "<": lambda a, b: a < b,
        ">": lambda a, b: a > b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
    }

    def __init__(self, tables: Dict[str, ITable]):
        self._tables = tables
        self._curr_table = None

    # TODO: Support joins
    # TODO: Support indexed lookup
    def plan(self, query: Query) -> Expr:
        if isinstance(query, Select):
            return self._plan_select(query)
        elif isinstance(query, Insert):
            return self._plan_insert(query)
        else:
            raise NotImplementedError()

    def _plan_select(self, query: Select) -> Expr:
        self._curr_table = self._tables[query.from_clause.table]

        expr = Seq(table.rows())

        if query.where_clause:
            expr = FilteredScan(expr, self._where_filter(query.where_clause))
        else:
            expr = Scan(expr)

        columns = self.table.schema.columnids(*query.exprs)
        if columns != table.schema.columnids():
            expr = ColumnProjection(expr, columns)

        return expr

    def _plan_insert(self, query: Insert):
        pass

    def _where_filter(self, clause: Where):
        cond = clause.condition
        if isinstance(cond, BinExpr):
            op = self.BINOPS[cond.op]
            left = self._operand(cond.left)
            right = self._operand(cond.right)
            return lambda row: op(left(row), right(row))
        else:
            raise NotImplementedError()

    def _operand(self, expr: Operand):
        if isinstance(expr, Const):
            return lambda row: expr.val
        elif isinstance(expr, Symbol):
            assert self._curr_table
            cid = self._curr_table.schema.columnid(expr.val)
            return lambda row: row[cid]
        else:
            raise NotImplementedError()
