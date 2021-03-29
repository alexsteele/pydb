import context

from pydb.table import (
    Schema,
    Column,
    ColumnAttr,
    DataType,
)
from pydb.query import (
    CreateTable,
    Insert,
    Select,
    From,
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
    def test_basic(self):
        db = MemDatabase("test")
        db.exec(CreateTable(SCHEMA))
        student = (0, "jane", 42)
        db.exec(
            Insert(
                "students",
                columns=("id", "name", "age"),
                values=student
            )
        )
        results = db.exec(
            Select(
                ("id", "name", "age"),
                From("students"),
            )
        )
        results = list(results)
        self.assertEqual(results, [student])

    def test_create_table_bad_schema(self):
        # empty schema
        # no primary key
        # multiple primary keys
        pass

    def test_create_table_already_exists(self):
        pass

    def test_select_bad_table(self):
        pass

    def test_select_bad_column_name(self):
        pass

    def test_select_no_filter(self):
        pass

    def test_select_column_subset(self):
        pass

    def test_where_multiple_matches(self):
        pass

    def test_where_key_lookup(self):
        # present key, missing key
        pass
