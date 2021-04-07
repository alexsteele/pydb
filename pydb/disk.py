from dataclasses import dataclass
from typing import (
    Iterator,
    Optional,
    Tuple,
    List,
    Sequence,
)
import struct
import pickle
import os.path
from .core import Database
from .index import HashIndex, Index
from .table import ITable, Schema


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

    def close(self):
        self._file.close()

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

    def scan(self, start=0) -> Iterator[Tuple[int, Tuple]]:
        self._file.seek(0, 2)
        end_offset = self._file.tell()
        self._file.seek(start)
        offset = start
        while offset < end_offset:
            yield offset, self.get()
            offset = self._file.tell()

    # TODO: allow append(), get() while iterating
    def __iter__(self) -> Iterator[Tuple[int, Tuple]]:
        return self.scan()

    @staticmethod
    def _encode_size(size: int) -> bytes:
        return struct.pack("<L", size)

    @staticmethod
    def _decode_size(s: bytes) -> int:
        assert len(s) == HeapFile.PREFIX_SIZE
        vals = struct.unpack("<L", s)
        assert len(vals) == 1
        return vals[0]


# TODO: Support secondary indexes
class DiskTable(ITable):
    def __init__(self, file: HeapFile, row_index: List[int]):
        self._file = file
        self._row_index = row_index  # rowid -> offset

    @staticmethod
    def open(name: str, folder: str):
        file = HeapFile.open(os.path.join(folder, name + ".data"))
        try:
            row_index = []
            for offset, _ in file.scan():
                row_index.append(offset)
            return DiskTable(file, row_index)
        except:
            file.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._file.close()

    def insert(self, row: Tuple) -> Tuple[int, Tuple]:
        offset = self._file.append(row)
        self._row_index.append(offset)
        return len(self._row_index) - 1, row

    def get(self, rowid: int) -> Optional[Tuple]:
        return self._file.get(self._row_index[rowid])

    def rows(self) -> Iterator[Tuple]:
        for offset, record in self._file.scan():
            yield record

    def indexes(self, column: Optional[str] = None) -> Sequence[Index]:
        return []


class DiskDatabase(Database):
    @staticmethod
    def open(path: str):
        pass
