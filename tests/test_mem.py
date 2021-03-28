import context

from pydb.table import (
    Schema,
    Column,
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
                    columns=[
                        Column("name", DataType.STRING),
                        Column("age", DataType.INT),
                    ],
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
