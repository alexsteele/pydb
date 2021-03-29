import context

from pydb.table import (
    Schema,
    DataType,
    Column,
    ColumnAttr,
)
from pydb.project import ColumnProjection
from pydb.plan import SimplePlanner
from pydb.query import (
    Select,
    From,
    Where,
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
        expr = self.planner.plan(query)
        expected = Scan(Rows(self.table))
        self.assertEqual(expr, expected)

    def test_column_projection(self):
        query = Select(("name", "age"), From("students"))
        expr = self.planner.plan(query)
        expected = ColumnProjection(
            Scan(Rows(self.table)), students.columnids(*("name", "age"))
        )
        self.assertEqual(expr, expected)

    def test_filtered_scan(self):
        pass
