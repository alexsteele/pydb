from dataclasses import dataclass
from typing import Iterator, Optional, Tuple

from .core import Database
from .index import HashIndex, Index
from .table import ITable, Schema


def save_index(index: Index, path: str):
    pass


class HeapFile:
    def __init__(self):
        pass

    def append(self, row: Tuple) -> int:
        pass

    def __iter__(self) -> Iterator[int, Tuple]:
        pass


@dataclass
class IndexSnapshot:
    row_index: List[int]
    col_indexes: List[HashIndex]

    def save(self, path: str):
        pass

    def load(self, path: str) -> Snapshot:
        pass


class DiskTable(ITable):
    def __init__(self, schema: Schema, file: HeapFile):
        self._schema = schema
        self._file = file
        self._row_index = []  # rowid -> offset
        self._col_indexes = []  # colid -> Index

    @staticmethod
    def open(self, root_path: str) -> DiskTable:
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

    def insert(self, row: Tuple) -> (int, Tuple):
        pass

    def get(self, rowid: int) -> Optional[Tuple]:
        pass


class DiskDatabase(Database):
    @staticmethod
    def open(path: str) -> DiskDatabase:
        pass
