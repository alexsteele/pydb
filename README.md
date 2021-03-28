# pydb

A tiny relational database in python
# Example

```
import pydb
from pydb.query import *

with pydb.open("mem:test") as db:
    db.exec(CreateTable("students", Column("id", DataType.STRING))
    db.exec(Insert("students", ("id"), ("foo")))
    result = db.exec(Select("id", From("students")))
    assert list(result) == [("foo")]
```
