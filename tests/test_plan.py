import context

from pydb.table import (
    Schema,
    DataType,
    Column,
    ColumnAttr,
)
from pydb.lookup import IndexedLookup
from pydb.project import ColumnProjection
from pydb.plan import SimplePlanner
from pydb.query import (
    Select,
    From,
    Where,
    Symbol,
    Const,
    BinExpr,
)
from pydb.scan import (
    Scan,
)
from pydb.expr import Rows
from pydb.mem import MemTable
import unittest

students = Schema(
    "students",
    Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT),
    Column("name", DataType.STRING),
    Column("age", DataType.INT),
)


class SimplePlannerTestCase(unittest.TestCase):
    def setUp(self):
        self.table = MemTable(students)
        self.planner = SimplePlanner({"students": self.table})

    def test_full_scan(self):
        query = Select(students.column_names(), From("students"))
        plan = self.planner.plan(query)
        expected = Scan(Rows(self.table))
        self.assertEqual(plan, expected)

    def test_column_projection(self):
        query = Select(("name", "age"), From("students"))
        plan = self.planner.plan(query)
        expected = ColumnProjection(
            Scan(Rows(self.table)), students.columnids("name", "age")
        )
        self.assertEqual(plan, expected)

    def test_filtered_scan(self):
        pass

    def test_indexed_lookup(self):
        assert any(self.table.indexes("id")), "missing index"
        query = Select(
            students.column_names(),
            From("students"),
            Where(BinExpr("=", Symbol("id"), Const(42))),
        )
        plan = self.planner.plan(query)
        index = self.table.indexes("id")[0]
        expected = IndexedLookup(self.table, index, 42)
        self.assertEqual(plan, expected)
