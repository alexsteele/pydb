# pydb

A tiny relational database in python
# Example

```
import pydb
from pydb.query import *
from pydb.table import *

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
```

# Setup

```
# Install requirements
$ pip install -r requirements.txt

# Run tests
$ python -m pytest
```
