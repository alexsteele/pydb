from typing import Dict, Optional, Tuple
from .expr import (
    ColumnProjection,
    Expr,
    Condition,
    BinOp,
    ColumnComp,
    ValueComp,
    FilteredScan,
    HashJoin,
    IndexedJoin,
    IndexedLookup,
    IndexedLookup,
    Scan,
)
import typing
from .query import (
    BinExpr,
    Const,
    Delete,
    Join,
    JoinKind,
    On,
    Operand,
    Query,
    Select,
    Symbol,
    Where,
)
from .table import ITable, Column


class Planner:
    def plan(self, query: Query) -> Expr:
        pass


class SimplePlanner:
    BINOPS = {
        "=": BinOp.EQ,
        "<": BinOp.LT,
        ">": BinOp.GT,
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
        if isinstance(query.from_clause.table, Join):
            assert not query.where_clause, "TODO: support where"
            expr = self._plan_join(query.from_clause.table)  # type: Expr
            return self._plan_join_projection(expr, query)
        else:
            table = self._tables[query.from_clause.table]
            self._curr_table = table
            if query.where_clause:
                expr = self._plan_where(table, query.where_clause)
            else:
                expr = Scan(table)
            columns = table.schema().columnids(*query.exprs)
            if columns != table.schema().columnids():
                expr = ColumnProjection(expr, columns)
        return expr

    def _plan_delete(self, query: Delete) -> Expr:
        pass

    def _plan_join(self, clause: Join):
        if len(clause.tables) != 2:
            raise ValueError("join: expected two tables")

        assert clause.kind == JoinKind.INNER, "TODO: support other join types"
        assert clause.condition, "TODO: support natural join"
        assert isinstance(clause.condition, On)

        condition = clause.condition.expr

        assert isinstance(condition, BinExpr)
        assert condition.op == "="
        assert isinstance(condition.left, Symbol)
        assert isinstance(condition.right, Symbol)

        """
        select t1.foo, t2.bar
        from t1 join t2 on t1.foo = t2.bar
                             ^       ^
                         cond.left cond.right
        """

        table1, col1 = self._find_join_column(condition.left.val)
        table2, col2 = self._find_join_column(condition.right.val)

        if any(table2.indexes(col2.name)):
            return IndexedJoin(
                Scan(table1),
                table1.schema().columnid(col1.name),
                table2.indexes(col2.name)[0],
                table2,
            )
        else:
            return HashJoin(
                Scan(table1),
                Scan(table2),
                table1.schema().columnid(col1.name),
                table2.schema().columnid(col2.name),
            )

    def _plan_join_projection(self, join_expr: Expr, query: Select) -> Expr:
        assert isinstance(query.from_clause.table, Join)
        column_indexes = []
        join_table_names = list(query.from_clause.table.tables)
        join_tables = [self._tables[name] for name in join_table_names]
        for colname in query.exprs:
            table, column = self._find_join_column(colname)
            if table.name() not in join_table_names:
                raise ValueError("unrecognized column {}".format(colname))
            table_idx = join_table_names.index(table.name())
            assert join_tables[table_idx] is table
            col_idx = sum(
                len(table.schema().columns) for table in join_tables[:table_idx]
            ) + join_tables[table_idx].schema().columnid(column.name)
            column_indexes.append(col_idx)
        return ColumnProjection(
            join_expr,
            tuple(column_indexes),
        )

    def _plan_where(self, table: ITable, clause: Where):
        assert clause.condition
        lookup = self._indexed_lookup(clause)
        if lookup:
            return lookup
        return FilteredScan(table, self._where_filter(clause))

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

    @typing.no_type_check
    def _where_filter(self, clause: Where) -> Condition:
        cond = clause.condition
        if isinstance(cond, BinExpr):
            op = self.BINOPS[cond.op]
            types = (type(cond.left), type(cond.right))
            assert self._curr_table
            schema = self._curr_table.schema()
            if types == (Symbol, Symbol):
                col1 = schema.columnid(cond.left.val)
                col2 = schema.columnid(cond.right.val)
                return ColumnComp(op, col1, col2)
            elif types == (Symbol, Const):
                col = schema.columnid(cond.left.val)
                val = cond.right.val
                return ValueComp(op, col, val)
            elif types == (Const, Symbol):
                val = cond.left.val
                col = schema.columnid(cond.right.val)
                return ValueComp(op, col, val)
            else:
                raise ValueError(
                    "unsupported expression types in where: {}".format(clause)
                )
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

    def _find_join_column(self, colname: str) -> Tuple[ITable, Column]:
        parts = colname.split(".")
        if len(parts) != 2:
            raise ValueError("column must be <table>.<column>: {}".format(colname))
        table_name, col_name = parts
        table = self._tables[table_name]
        column = table.schema().column(col_name)
        return table, column
