from . import context
from .base import DatabaseTestCase

import unittest
from pydb.mem import MemDatabase, MemTable
from pydb.table import Column, ColumnAttr, DataType, Schema


class MemTableTestCase(unittest.TestCase):
    def test_mem_table(self):
        schema = Schema(
            "students",
            Column(
                "id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT
            ),
            Column("name", DataType.STRING),
            Column("age", DataType.INT),
        )
        table = MemTable(schema)
        self.assertEqual(table.schema(), schema)
        self.assertEqual(table.get(0), None)
        self.assertEqual(list(table.rows()), [])
        self.assertEqual(len(table.indexes()), 1)

        student = (0, "rohit", 20)
        rowid, row = table.insert(student)
        self.assertEqual(row, student)
        self.assertEqual(table.get(rowid), row)
        self.assertEqual(list(table.rows()), [row])


class MemDatabaseTestCase(DatabaseTestCase, unittest.TestCase):
    def setUp(self):
        self.initdb(MemDatabase("test"))
