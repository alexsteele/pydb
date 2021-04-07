from . import context

import unittest
from pydb.error import SchemaError
from pydb.core import Database
from pydb.mem import MemDatabase, MemTable
from pydb.query import (
    BinExpr,
    Const,
    CreateTable,
    From,
    Insert,
    Select,
    Symbol,
    Where,
    Join,
    On,
)
from pydb.table import Column, ColumnAttr, DataType, Schema

SCHEMA = Schema(
    "students",
    Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT),
    Column("name", DataType.STRING),
    Column("age", DataType.INT),
)


class DatabaseTestCase:
    """Common tests for database implementations"""

    def initdb(self, db: Database):
        """Must be called in setUp()"""
        self.db = db
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

    def test_select_join(self):
        signups_schema = Schema(
            "signups",
            Column(
                "id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT
            ),
            Column("sid", DataType.INT),
            Column("timestamp", DataType.INT),
        )
        self.db.exec(CreateTable(signups_schema))
        students = [
            (0, "abe", 20),
            (1, "bark", 30),
            (2, "cab", 40),
        ]
        signups = [
            (0, 0, 100),
            (1, 1, 101),
            (2, 2, 103),
        ]
        for student in students:
            self._insert(student)
        for signup in signups:
            self.db.exec(Insert("signups", signups_schema.column_names(), signup))
        results = self.db.exec(
            Select(
                ("students.name", "signups.timestamp"),
                From(
                    Join(
                        ("students", "signups"),
                        On(BinExpr("=", Symbol("students.id"), Symbol("signups.sid"))),
                    )
                ),
            )
        )
        expected = [
            ("abe", 100),
            ("bark", 101),
            ("cab", 103),
        ]
        self.assertEqual(list(results), expected)

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
