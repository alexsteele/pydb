from .core import Database
from .parse import parse_query
from .query import CreateTable
from .plan import SimplePlanner
from .table import ITable
from typing import List, Tuple


class MemTable(ITable):
    def __init__(self, schema):
        self._schema = schema
        self._rows = []  # type: List[Tuple]
        self._indexes = []

    def schema(self):
        return self._schema

    def rows(self):
        return iter(self._rows)

class MemDatabase(Database):
    def __init__(self, name):
        self._name = name
        self._tables = {}  # type: Dict[str, MemTable]

    def exec(self, query, **options):
        if isinstance(query, str):
            query = parse_query(query)
        if isinstance(query, CreateTable):
            self._create_table(query)
            return ()
        planner = SimplePlanner(self._tables)
        expr = planner.plan(query)
        return expr.exec()

    def _create_table(self, query: CreateTable):
        if query.schema.name in self._tables:
            raise ValueError("{} already exists".format(query.schema.name))
        table = MemTable(query.schema)
        self._tables[query.schema.name] = table
