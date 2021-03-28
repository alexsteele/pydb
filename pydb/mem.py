from .core import Database, Cursor
from .parse import parse_query
from .query import (
    CreateTable,
    Insert,
    Select,
)
from .plan import SimplePlanner
from .table import (
    ITable,
    Schema,
    Sequence,
    Any,
)
from typing import List, Tuple, Iterable


def check_col_types(row: Sequence[Any], schema: Schema):
    assert len(row) == len(schema.columns)


class MemCursor(Cursor):
    def __init__(self, rows: Iterable[Tuple]):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class MemTable(ITable):
    def __init__(self, schema):
        self._schema = schema
        self._rows = []  # type: List[Tuple]
        self._indexes = []

    @staticmethod
    def create(schema: Schema):
        # TODO: create indexes
        return MemTable(schema)

    def schema(self):
        return self._schema

    def rows(self):
        return iter(self._rows)

    def insert(self, query: Insert) -> Tuple:
        if len(query.columns) != len(query.values):
            raise ValueError("columns don't match values")
        if query.columns != self._schema.column_names():
            # TODO: Support NULL, autoinc, etc.
            raise ValueError("columns don't match schema")
        check_col_types(query.values, self._schema)
        # TODO: check key uniqueness
        values = tuple(query.values)
        self._rows.append(values)
        return values


class MemDatabase(Database):
    def __init__(self, name):
        self._name = name
        self._tables = {}  # type: Dict[str, MemTable]

    def exec(self, query, **options):
        if isinstance(query, str):
            query = parse_query(query)
        if isinstance(query, CreateTable):
            return self._create_table(query)
        if isinstance(query, Select):
            return self._select(query)
        if isinstance(query, Insert):
            return self._insert(query)
        raise NotImplementedError("unsupported query type: {}".format(type(query)))

    def _create_table(self, query: CreateTable) -> Cursor:
        if query.schema.name in self._tables:
            raise ValueError("{} already exists".format(query.schema.name))
        self._tables[query.schema.name] = MemTable.create(query.schema)
        return MemCursor(())

    def _select(self, query: Select) -> Cursor:
        planner = SimplePlanner(self._tables)
        expr = planner.plan(query)
        return MemCursor(expr.exec())

    def _insert(self, query: Insert) -> Cursor:
        if query.table not in self._tables:
            raise ValueError("unrecognized table {}".format(query.table))
        table = self._tables[query.table]
        return table.insert(query)
