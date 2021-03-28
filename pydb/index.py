import bisect
from typing import (
    TypeVar,
    Optional,
    Iterator,
    Dict,
    Generic,
)

K = TypeVar("K")
V = TypeVar("T")


class Index(Generic[K, V]):
    def insert(self, key: K, val: V):
        pass

    def find(self, key: K) -> Optional[V]:
        pass


class SortedIndex(Index):
    def find(self, key):
        return next(self.scan(key), None)

    def scan(self, key: K) -> Iterator[V]:
        pass

    def rscan(self, key: K) -> Iterator[V]:
        pass


class RangeIndex(Index):
    def scan(self, start: K, end: K) -> Iterator[V]:
        pass


class HashIndex(Index):
    def __init__(self, index: Dict[K, V]):
        self._index = index

    def query(self, key):
        return self._index.get(key, None)


class SortedSeqIndex(SortedIndex):
    def __init__(self, seq):
        self._seq = seq

    def scan(self, key):
        idx = bisect.bisect_left(seq, key)
        return iter(self._seq[idx:])

    def rscan(self, key):
        idx = bisect.bisect_right(seq, key)
        return reversed(self._seq[:idx])
