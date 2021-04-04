from . import context
import unittest
import pydb
from pydb.query import CreateTable, From, Insert, Select
from pydb.table import Column, ColumnAttr, DataType, Schema


class PydbInterfaceTestCase(unittest.TestCase):
    def test_readme_example(self):
        schema = Schema(
            "students",
            Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY),
            Column("name", DataType.STRING),
        )

        with pydb.connect("mem:test") as db:
            db.exec(CreateTable(schema))
            db.exec(Insert("students", ("id", "name"), (0, "ack")))
            result = db.exec(Select(("id", "name"), From("students")))
            assert list(result) == [(0, "ack")]
