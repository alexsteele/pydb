from typing import Iterable, List, Tuple

from .core import Cursor, Database
from .index import HashIndex, Index
from .parse import parse_query
from .plan import SimplePlanner
from .query import CreateTable, Insert, Select
from .table import Column, ColumnAttr, Dict, ITable, Schema


class MemCursor(Cursor):
    def __init__(self, rows: Iterable[Tuple]):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class MemTable(ITable):
    def __init__(self, schema):
        self._schema = schema
        self._rows = []  # type: List[Tuple]
        self._indexes = self._init_indexes(schema)

    # TODO: Consider indexing indexes by column id, not name
    # TODO: Consider supporting > 1 index per column
    def _init_indexes(self, schema: Schema) -> Dict[str, Index]:
        return {
            col.name: HashIndex() for col in schema.columns if self._should_index(col)
        }

    def _should_index(self, col: Column) -> bool:
        return any(
            col.hasattr(attr) for attr in (ColumnAttr.PRIMARY_KEY, ColumnAttr.UNIQUE)
        )

    # TODO: Validate row dtypes and constraints
    # TODO: Populate defaults for autoinc
    # TODO: Handle index.insert() failures
    def insert(self, row: Tuple) -> Tuple[int, Tuple]:
        assert len(row) == len(self._schema.columns)
        rowid = len(self._rows)
        for column, index in self._indexes.items():
            val = row[self._schema.columnid(column)]
            index.insert(val, rowid)
        self._rows.append(row)
        return (rowid, row)

    def get(self, rowid):
        return self._rows[rowid] if rowid in range(len(self._rows)) else None

    def indexes(self, column=None):
        if not column:
            return list(self._indexes.values())
        index = self._indexes.get(column, None)
        return (index,) if index else ()

    def schema(self):
        return self._schema

    def rows(self):
        return iter(self._rows)


class MemDatabase(Database):
    def __init__(self, name):
        self._name = name
        self._tables = {}  # type: Dict[str, ITable]

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
        self._tables[query.schema.name] = MemTable(query.schema)
        return MemCursor(())

    def _select(self, query: Select) -> Cursor:
        planner = SimplePlanner(self._tables)
        expr = planner.plan(query)
        return MemCursor(expr.exec())

    def _insert(self, query: Insert) -> Cursor:
        if len(query.columns) != len(query.values):
            raise ValueError("columns don't match values")
        table = self._tables.get(query.table, None)
        if not table:
            raise ValueError("unrecognized table {}".format(query.table))
        if query.columns != table.schema().column_names():
            # TODO: Support auto-populated columns
            raise ValueError("columns don't match schema")
        rowid, row = table.insert(tuple(query.values))
        return MemCursor((row))
