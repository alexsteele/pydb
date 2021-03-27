from .core import Database
from .parse import parse_query
from .plan import Plan, Planner
from .index import SortedIndex
from .scan import IndexedLookup
from .project import Projection, ColumnProjection
from .filter import Filter
from .table import ITable


class MemTable(ITable):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.rows = []
        self.indexes = []


# TODO: fixme
class MemExecutor:
    def __init__(self):
        pass

    def exec(self):
        table = MemTable()
        index = HashIndex()
        # lookup = IndexedLookup(table.rows, index, )

        # projection = ColumnProjection(lookup.exec)


class MemDatabase(Database):
    def __init__(self, name):
        self._name = name
        self._tables = {}

    def query(self, query, **options):
        if isinstance(query, str):
            query = parse_query(query)
        pass
