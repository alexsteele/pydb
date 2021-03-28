from .core import Database, Cursor
from .parse import parse_query
from .query import (
    CreateTable,
    Insert,
)
from .plan import SimplePlanner
from .table import (
    ITable,
    Schema,
)
from typing import List, Tuple


def check_col_types(row: Tuple, schema: Schema):
    assert len(row) == len(schema.columns)


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
            print(query.colum)
            raise ValueError("columns don't match schema")
        check_col_types(query.values, self._schema)
        # TODO: check key uniqueness
        self._rows.append(query.values)
        return query.values


class MemDatabase(Database):
    def __init__(self, name):
        self._name = name
        self._tables = {}  # type: Dict[str, MemTable]

    def exec(self, query, **options):
        if isinstance(query, str):
            query = parse_query(query)
        if isinstance(query, CreateTable):
            return self._create_table(query)
        if isinstance(query, Insert):
            return self._insert(query)
        planner = SimplePlanner(self._tables)
        expr = planner.plan(query)
        return expr.exec()

    def _create_table(self, query: CreateTable) -> Cursor:
        if query.schema.name in self._tables:
            raise ValueError("{} already exists".format(query.schema.name))
        self._tables[query.schema.name] = MemTable.create(query.schema)
        return ()

    def _insert(self, query: Insert) -> Cursor:
        if query.table not in self._tables:
            raise ValueError("unrecognized table {}".format(query.table))
        table = self._tables[query.table]
        return table.insert(query)
