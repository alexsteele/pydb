import context

from pydb.errors import SchemaError
from pydb.table import (
    Schema,
    Column,
    ColumnAttr,
    DataType,
)
from pydb.query import (
    BinExpr,
    CreateTable,
    Insert,
    Select,
    From,
    Where,
    Symbol,
    Const,
)
from pydb.mem import MemDatabase, MemTable
import unittest

SCHEMA = Schema(
    "students",
    Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT),
    Column("name", DataType.STRING),
    Column("age", DataType.INT),
)


class MemTableTestCase(unittest.TestCase):
    def test_mem_table(self):
        table = MemTable(SCHEMA)
        self.assertEqual(table.schema(), SCHEMA)
        self.assertEqual(table.get(0), None)
        self.assertEqual(list(table.rows()), [])
        self.assertEqual(len(table.indexes()), 1)

        student = (0, "rohit", 20)
        rowid, row = table.insert(student)
        self.assertEqual(row, student)
        self.assertEqual(table.get(rowid), row)
        self.assertEqual(list(table.rows()), [row])


class MemDatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db = MemDatabase("test")
        self.db.exec(CreateTable(SCHEMA))

    def test_simple_select(self):
        students = [
            (0, "ark", 42),
            (1, "bam", 43),
        ]
        self._insert(*students)
        results = self.db.exec(Select(SCHEMA.column_names(), From("students")))
        self.assertEqual(list(results), students)

    def test_select_by_primary_key(self):
        students = [
            (0, "ark", 10),
            (1, "bam", 11),
            (2, "cam", 12),
        ]
        self._insert(*students)
        results = self.db.exec(
            Select(
                SCHEMA.column_names(),
                From("students"),
                Where(BinExpr("=", Symbol("id"), Const(1))),
            )
        )
        self.assertEqual(list(results), [students[1]])

    def test_select_with_name_filter(self):
        students = [
            (0, "ark", 10),
            (1, "bam", 11),
            (2, "bam", 12),
        ]
        self._insert(*students)
        results = self.db.exec(
            Select(
                SCHEMA.column_names(),
                From("students"),
                Where(BinExpr("=", Symbol("name"), Const("bam"))),
            )
        )
        self.assertEqual(list(results), students[1:])

    def test_select_column_subset(self):
        assert "name" in SCHEMA.column_names()
        assert "age" in SCHEMA.column_names()
        student = (0, "ark", 10)
        self._insert(student)
        results = self.db.exec(
            Select(
                ("name", "age"),
                From("students"),
            )
        )
        self.assertEqual(list(results), [("ark", 10)])

    def test_create_table_bad_schema(self):
        with self.assertRaises(SchemaError):
            self.db.exec(CreateTable(Schema("")))
        with self.assertRaises(SchemaError):
            self.db.exec(CreateTable(Schema("foo")))
        with self.assertRaises(SchemaError):
            self.db.exec(
                CreateTable(
                    Schema(
                        "foo",
                        Column("bar", DataType.STRING),
                        Column("bar", DataType.INT),
                    )
                )
            )

    def test_create_table_already_exists(self):
        pass

    def test_select_bad_table(self):
        pass

    def test_select_bad_column_name(self):
        pass

    def _insert(self, *rows):
        results = []
        for row in rows:
            results.append(self.db.exec(Insert("students", SCHEMA.column_names(), row)))
        return results
