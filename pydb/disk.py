from dataclasses import dataclass
from typing import (
    Iterator,
    Optional,
    Tuple,
    List,
    Sequence,
)
import io
import os
import pickle
import struct
from .core import Database
from .index import Index
from .table import ITable, Schema
from .core import Cursor, Database
from .index import Index
from .parse import parse_query
from .plan import SimplePlanner
from .query import CreateTable, Insert, Select
from .table import Dict, ITable, Schema, check_schema


@dataclass
class RowHeader:
    SIZE = 5
    size: int
    tombstone: bool

    def write(self, w):
        w.write(struct.pack("<L?", self.size, self.tombstone))

    @classmethod
    def read(cls, r):
        size, tombstone = struct.unpack("<L?", r.read(cls.SIZE))
        return RowHeader(size, tombstone)


# TODO: Minimize seeks
# TODO: Prefix file with HEADER
class HeapFile:
    """
    - not threadsafe
    - at most one open instance of the file at any time
    """

    MAGIK = "hfmagik"
    VERSION = 1
    HEADER = "{} v{}".format(MAGIK, VERSION).encode("utf-8")
    PREFIX_SIZE = 4

    def __init__(self, file):
        self._file = file

    @staticmethod
    def open(path: str):
        # We want read/write/create if DNE. Incredibly, no mode seems to
        # accomplish this. WTF. I thought I understood you unix. Alas,
        # it's not me it's you.
        #   'r' modes only work if it exists
        #   'w' modes truncate if it exists
        #   'a' modes only allow appends so seek()+write() doesn't work D:
        mode = "rb+" if os.path.exists(path) else "wb+"
        return HeapFile(open(path, mode))

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._file.close()

    def append(self, record: Tuple) -> int:
        self._file.seek(0, io.SEEK_END)  # TODO: avoid seek
        offset = self._file.tell()
        data = pickle.dumps(record)
        header = RowHeader(size=len(data), tombstone=False)
        header.write(self._file)
        self._file.write(data)
        return offset

    def get(self, offset: int) -> Tuple:
        self._file.seek(offset)
        header, record = self._iget()
        if header.tombstone:
            raise ValueError("row is removed")
        return record

    def _iget(self):
        header = RowHeader.read(self._file)
        if header.tombstone:
            self._file.seek(header.size, io.SEEK_CUR)
            return header, None
        data = self._file.read(header.size)
        record = pickle.loads(data)
        return header, record

    def remove(self, offset: int):
        self._file.seek(offset)
        header = RowHeader.read(self._file)
        if not header.tombstone:
            header.tombstone = True
            self._file.seek(offset)
            header.write(self._file)

    def scan(self, start=0) -> Iterator[Tuple[int, Tuple]]:
        self._file.seek(0, io.SEEK_END)
        end = self._file.tell()
        self._file.seek(start)
        offset = start
        while offset < end:
            header, record = self._iget()
            if not header.tombstone:
                yield offset, record
            offset = self._file.tell()

    # TODO: allow append(), get() while iterating
    def __iter__(self) -> Iterator[Tuple[int, Tuple]]:
        return self.scan()


# TODO: Support secondary indexes
class DiskTable(ITable):
    def __init__(self, schema: Schema, file: HeapFile, row_index: List[Optional[int]]):
        self._schema = schema
        self._file = file
        self._row_index = row_index  # rowid -> offset. offset = None for deleted rows

    @staticmethod
    def open(schema: Schema, folder: str):
        file = HeapFile.open(os.path.join(folder, schema.name + ".data"))
        try:
            row_index = []
            for offset, _ in file.scan():
                row_index.append(offset)
            return DiskTable(schema, file, row_index)
        except:
            file.close()
            raise

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def schema(self):
        return self._schema

    def insert(self, row: Tuple) -> Tuple[int, Tuple]:
        offset = self._file.append(row)
        self._row_index.append(offset)
        return len(self._row_index) - 1, row

    def delete(self, rowid: int):
        offset = self._row_index[rowid]
        if offset is None:
            raise ValueError("rowid {} does not exist".format(rowid))
        self._file.remove(offset)
        self._row_index[rowid] = None

    def get(self, rowid: int) -> Optional[Tuple]:
        offset = self._row_index[rowid]
        if offset is None:
            return None
        return self._file.get(offset)

    def rows(self) -> Iterator[Tuple]:
        for offset, record in self._file.scan():
            yield record

    def indexes(self, column: Optional[str] = None) -> Sequence[Index]:
        return []


# TODO: Factor out code shared with MemDatabase?
class DiskDatabase(Database):
    def __init__(self, folder: str, tables: Dict[str, ITable]):
        self._folder = folder
        self._tables = tables

    def exec(self, query, **options):
        if isinstance(query, str):
            query = parse_query(query)
        if isinstance(query, CreateTable):
            return self._create_table(query)
        if isinstance(query, Select):
            return self._select(query)
        if isinstance(query, Insert):
            return self._insert(query)
        raise NotImplementedError("unsupported query type: {}".format(type(query)))

    @classmethod
    def open(cls, folder):
        folder = os.path.abspath(folder)
        cls._create_folder(folder)
        manifest = cls._load_manifest(folder)
        tables = cls._open_tables(folder, manifest)
        return DiskDatabase(folder, tables)

    # TODO: Clean up error handling
    def close(self):
        error = None
        for table in self._tables.values():
            try:
                table.close()
            except Exception as err:
                error = err
        try:
            self._save_manifest()
        except Exception as err:
            error = err
        if error:
            raise error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _create_table(self, query: CreateTable) -> Cursor:
        check_schema(query.schema)
        if query.schema.name in self._tables:
            raise ValueError("{} already exists".format(query.schema.name))
        self._tables[query.schema.name] = DiskTable.open(query.schema, self._folder)
        return tuple()

    def _select(self, query: Select) -> Cursor:
        planner = SimplePlanner(self._tables)
        expr = planner.plan(query)
        return expr.exec()

    def _insert(self, query: Insert) -> Cursor:
        if len(query.columns) != len(query.values):
            raise ValueError("columns don't match values")
        table = self._tables.get(query.table, None)
        if not table:
            raise ValueError("unrecognized table {}".format(query.table))
        if query.columns != table.schema().column_names():
            # TODO: Support auto-populated columns
            raise ValueError("columns don't match schema")
        rowid, row = table.insert(tuple(query.values))
        return (row,)

    @staticmethod
    def _create_folder(folder):
        assert os.path.isabs(folder)
        parent = os.path.dirname(folder)
        if not os.path.exists(parent):
            raise ValueError("missing parent folder {}".format(parent))
        if not os.path.exists(folder):
            os.mkdir(folder)

    @staticmethod
    def _load_manifest(folder):
        manifest_path = os.path.join(folder, "MANIFEST")
        if not os.path.exists(manifest_path):
            return {"table_schemas": []}
        with open(manifest_path, "rb") as f:
            manifest = pickle.load(f)
        assert isinstance(manifest, dict)
        assert "table_schemas" in manifest
        return manifest

    def _save_manifest(self):
        manifest = {
            "table_schemas": [table.schema() for table in self._tables.values()]
        }
        manifest_path = os.path.join(self._folder, "MANIFEST")
        with open(manifest_path, "wb") as f:
            pickle.dump(manifest, f)

    @staticmethod
    def _open_tables(folder, manifest):
        tables = {}
        try:
            for schema in manifest["table_schemas"]:
                tables[schema.name] = DiskTable.open(schema, folder)
        except:
            for table in tables.values():
                try:
                    table.close()
                except:
                    pass
            raise
        return tables
