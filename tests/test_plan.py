from . import context
import unittest
from pydb.expr import (
    ColumnProjection,
    HashJoin,
    IndexedJoin,
    IndexedLookup,
    Scan,
)
from pydb.mem import MemTable
from pydb.plan import SimplePlanner
from pydb.query import (
    BinExpr,
    Const,
    From,
    Join,
    JoinKind,
    On,
    Select,
    Symbol,
    Where,
)
from pydb.table import Column, ColumnAttr, DataType, Schema

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
        expected = Scan(self.table)
        self.assertEqual(plan, expected)

    def test_column_projection(self):
        query = Select(("name", "age"), From("students"))
        plan = self.planner.plan(query)
        expected = ColumnProjection(
            Scan(self.table), students.columnids("name", "age")
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

    @unittest.skip
    def test_natural_join(self):
        raise NotImplementedError()

    def test_hash_join(self):
        signups = Schema(
            "signups",
            Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY),
            Column("name", DataType.STRING),
        )
        table2 = MemTable(signups)
        query = Select(
            ("students.id", "signups.id"),
            From(
                Join(
                    ("students", "signups"),
                    On(BinExpr("=", Symbol("students.name"), Symbol("signups.name"))),
                    JoinKind.INNER,
                )
            ),
        )
        expected = ColumnProjection(
            HashJoin(
                Scan(self.table),
                Scan(table2),
                students.columnid("name"),
                signups.columnid("name"),
            ),
            (students.columnid("id"), len(students.columns) + signups.columnid("id")),
        )
        planner = SimplePlanner({"students": self.table, "signups": table2})
        plan = planner.plan(query)
        self.assertEqual(plan, expected)

    def test_indexed_join(self):
        table2 = MemTable(
            Schema("signups", Column("sid", DataType.INT, ColumnAttr.PRIMARY_KEY))
        )
        query = Select(
            ("students.name", "signups.sid"),
            From(
                Join(
                    ("students", "signups"),
                    On(BinExpr("=", Symbol("students.id"), Symbol("signups.sid"))),
                    JoinKind.INNER,
                )
            ),
        )
        assert any(table2.indexes("sid"))
        expected = ColumnProjection(
            IndexedJoin(
                Scan(self.table),
                table2.schema().columnid("sid"),
                table2.indexes("sid")[0],
                table2,
            ),
            (
                students.columnid("name"),
                len(students.columns) + table2.schema().columnid("sid"),
            ),
        )
        planner = SimplePlanner({"students": self.table, "signups": table2})
        plan = planner.plan(query)
        self.assertEqual(plan, expected)

    @unittest.skip
    def test_join_where(self):
        raise NotImplementedError()
