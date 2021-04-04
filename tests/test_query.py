from . import context
import unittest
from pydb.query import *


class QuerySyntaxTestCase(unittest.TestCase):
    def test_select_syntax(self):
        query = Select(
            ("id", "name"),
            From("students"),
            Where(BinExpr("=", Symbol("id"), Const(5))),
        )
        assert query

        query = Select(
            ("students.id", "students.name", "signups.timestamp"),
            From(
                Join(
                    ("students", "signups"),
                    On(
                        BinExpr("=", Symbol("students.id"), Symbol("signups.studentid"))
                    ),
                )
            ),
        )
        assert query
