import context

from pydb.table import (
    Schema,
    Column,
    DataType,
)
from pydb.query import (
    CreateTable,
)
from pydb.memdb import MemDatabase
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
        # db.exec()
        self.assertEqual(1, 1)
