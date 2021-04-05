from dataclasses import dataclass
from typing import (
    Iterator,
    Optional,
    Tuple,
    List,
)
import struct
import pickle
from .core import Database
from .index import HashIndex, Index
from .table import ITable, Schema


def save_index(index: Index, path: str):
    pass


class HeapFile:
    """
    - not threadsafe
    - at most one open instance of the file at any time
    """

    # TODO: prefix file with header
    MAGIK = "hfmagik"
    VERSION = 1
    HEADER = "{} v{}".format(MAGIK, VERSION).encode("utf-8")
    PREFIX_SIZE = 4

    def __init__(self, file):
        self._file = file

    @staticmethod
    def open(path: str):
        return HeapFile(open(path, "ab+"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._file.close()

    def append(self, record: Tuple) -> int:

        # seek to end. TODO: avoid
        self._file.seek(0, 2)

        data = pickle.dumps(record)
        size = self._encode_size(len(data))
        offset = self._file.tell()

        assert len(size) == self.PREFIX_SIZE

        self._file.write(size)
        self._file.write(data)

        return offset

    def get(self, offset: int = -1) -> Tuple:
        if offset != -1:
            self._file.seek(offset)
            return self.get()

        size = self._decode_size(self._file.read(self.PREFIX_SIZE))
        data = self._file.read(size)
        record = pickle.loads(data)
        return record

    # TODO: allow append(), get() while iterating
    def __iter__(self) -> Iterator[Tuple[int, Tuple]]:
        self._file.seek(0, 2)
        end_offset = self._file.tell()
        self._file.seek(0)
        offset = 0
        while offset < end_offset:
            yield offset, self.get()
            offset = self._file.tell()

    @staticmethod
    def _encode_size(size: int) -> bytes:
        return struct.pack("<L", size)

    @staticmethod
    def _decode_size(s: bytes) -> int:
        assert len(s) == HeapFile.PREFIX_SIZE
        vals = struct.unpack("<L", s)
        assert len(vals) == 1
        return vals[0]


@dataclass
class IndexSnapshot:
    row_index: List[int]
    col_indexes: List[HashIndex]

    def save(self, path: str):
        pass

    def load(self, path: str):
        pass


class DiskTable(ITable):
    def __init__(self, schema: Schema, file: HeapFile):
        self._schema = schema
        self._file = file
        self._row_index = []  # type: List[int] # rowid -> offset
        self._col_indexes = []  # type: List[Index] # colid -> Index

    @staticmethod
    def open(self, root_path: str):
        # if table exists
        #   rebuild indexes
        # else
        #   create heapfile
        #   create snapshot file
        pass

    def snapshot(self):
        pass

    def close(self):
        pass

    def insert(self, row: Tuple) -> Tuple[int, Tuple]:
        pass

    def get(self, rowid: int) -> Optional[Tuple]:
        pass


class DiskDatabase(Database):
    @staticmethod
    def open(path: str):
        pass
