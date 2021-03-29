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
from pydb.mem import MemDatabase
import unittest


class MemDatabaseTestCase(unittest.TestCase):
    def test_basic(self):
        db = MemDatabase("test")
        db.exec(
            CreateTable(
                Schema(
                    "students",
                    Column("name", DataType.STRING),
                    Column("age", DataType.INT),
                )
            )
        )
        db.exec(
            Insert(
                "students",
                ("name", "age"),
                ("jane", 10),
            )
        )
        results = db.exec(
            Select(
                ("name", "age"),
                From("students"),
            )
        )
        results = list(results)
        self.assertEqual(results, [("jane", 10)])

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
