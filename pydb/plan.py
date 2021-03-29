from typing import Dict, Optional, Tuple

from .expr import Expr, Rows
from .lookup import IndexedLookup
from .project import ColumnProjection
from .query import BinExpr, Const, Operand, Query, Select, Symbol, Where
from .scan import FilteredScan, Scan
from .table import ITable


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
        self._curr_table = None  # type: Optional[ITable]

    def plan(self, query: Query) -> Expr:
        if isinstance(query, Select):
            return self._plan_select(query)
        else:
            raise NotImplementedError("unsupported query type {}".format(query))

    def _plan_select(self, query: Select) -> Expr:
        table = self._tables[query.from_clause.table]
        self._curr_table = table

        expr = Rows(table)  # type: Expr

        if query.where_clause:
            expr = self._plan_where(expr, query.where_clause)
        else:
            expr = Scan(expr)

        columns = table.schema().columnids(*query.exprs)
        if columns != table.schema().columnids():
            expr = ColumnProjection(expr, columns)

        return expr

    def _plan_where(self, expr: Expr, clause: Where):
        assert clause.condition
        lookup = self._indexed_lookup(clause)
        if lookup:
            return lookup
        return FilteredScan(expr, self._where_filter(clause))

    def _indexed_lookup(self, clause: Where) -> Optional[Expr]:
        assert self._curr_table
        column, key = self._extract_key_col(clause)
        index = next(iter(self._curr_table.indexes(column)), None)
        if not index:
            return None
        return IndexedLookup(self._curr_table, index, key)

    def _extract_key_col(self, clause: Where) -> Tuple[Optional[str], Optional[str]]:
        cond = clause.condition
        if not isinstance(cond, BinExpr):
            return (None, None)
        if cond.op != "=":
            return (None, None)
        shape = (type(cond.left), type(cond.right))
        if shape == (Symbol, Const):
            assert isinstance(cond.left, Symbol)
            assert isinstance(cond.right, Const)
            return (cond.left.val, cond.right.val)
        if shape == (Const, Symbol):
            assert isinstance(cond.left, Const)
            assert isinstance(cond.right, Symbol)
            return (cond.right.val, cond.left.val)
        return (None, None)

    # TODO: Turn this into a static filter/Expr
    def _where_filter(self, clause: Where):
        cond = clause.condition
        if isinstance(cond, BinExpr):
            op = self.BINOPS[cond.op]
            left = self._operand(cond.left)
            right = self._operand(cond.right)
            return lambda row: op(left(row), right(row))
        else:
            raise NotImplementedError()

    def _operand(self, op: Operand):
        if isinstance(op, Const):
            return lambda row: op.val
        elif isinstance(op, Symbol):
            assert self._curr_table
            cid = self._curr_table.schema().columnid(op.val)
            return lambda row: row[cid]
        else:
            raise NotImplementedError("unexpected operand {}".format(op))
