from . import context
import pydb
from pydb.query import *
from pydb.table import *
from collections import defaultdict
import string
import random
import time

USERS_SCHEMA = Schema(
    "users",
    Column("id", DataType.INT, ColumnAttr.PRIMARY_KEY, ColumnAttr.AUTO_INCREMENT),
    Column("name", DataType.STRING),
)


class Fuzzer:
    def __init__(
        self,
        db: pydb.Database,
        duration: int = 5,
        max_iters: int = int(1e6),
        max_size: int = 128_000,
    ):
        self.db = db
        self.duration = duration
        self.max_iters = max_iters
        self.max_size = max_size
        self.users = []
        self.stats = defaultdict(lambda: 0)
        self.checks_failed = defaultdict(lambda: 0)

    def run(self):
        print("fuzzer: starting run")
        start = time.time()
        self.db.exec(CreateTable(USERS_SCHEMA))
        while (
            time.time() - start < self.duration and self.stats["iters"] < self.max_iters
        ):
            self._run_one()
        self.stats["duration"] = int(time.time() - start)
        self._report_results()

    def _report_results(self):
        from pprint import pprint

        print("stats:")
        pprint(dict(self.stats))
        if self.stats["checks_failed"] > 0:
            print("checks failed:")
            pprint(dict(self.checks_failed))
            print("FAIL")
        else:
            print("PASS")

    def _run_one(self):
        r = random.randint(0, 100)
        if r < 50:
            self._insert()
        else:
            self._select()
        self.stats["iters"] += 1

    def _insert(self):
        if len(self.users) >= self.max_size:
            return
        user = self._rand_user()
        self.db.exec(Insert("users", USERS_SCHEMA.column_names(), user))
        self.users.append(user)
        self.stats["inserts"] += 1

    def _select(self):
        if len(self.users) == 0:
            return
        r = random.randint(0, 100)
        if r < 50:
            self._select_by_id()
        else:
            self._select_by_name()

    def _select_by_id(self):
        def select(user_id):
            return self.db.exec(
                Select(
                    USERS_SCHEMA.column_names(),
                    From("users"),
                    Where(BinExpr("=", Symbol("id"), Const(user_id))),
                )
            )

        r = random.randint(0, 100)
        if r < 25:
            user_id = len(self.users) + random.randint(1, 99999)
            self._check(list(select(user_id)) == [], "select_by_id_miss")
        else:
            user = random.choice(self.users)
            user_id = user[0]
            results = list(select(user_id))
            self._check(results == [user], "select_by_id_hit")
        self.stats["select_by_id"] += 1

    def _select_by_name(self):
        pass

    def _check(self, cond: bool, name):
        if not cond:
            self.stats["checks_failed"] += 1
            self.checks_failed[name] += 1
        else:
            self.stats["checks_passed"] += 1
        return cond

    def _rand_user(self):
        return (
            len(self.users),
            "".join(random.choice(string.ascii_letters) for _ in range(32)),
        )
