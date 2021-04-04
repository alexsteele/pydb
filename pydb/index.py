import bisect
from typing import Dict, Generic, Iterator, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class Index(Generic[K, V]):
    def find(self, key: K) -> Optional[V]:
        pass

    def insert(self, key: K, val: V):
        pass

    def update(self, key: K, val, V) -> Optional[V]:
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
    def __init__(self, index: Dict[K, V] = None):
        self._index = index or {}

    def insert(self, key, val):
        if key in self._index:
            raise ValueError("duplicate key {}".format(key))
        self._index[key] = val

    def update(self, key, val):
        old = self._index.get(key, None)
        self._index[key] = val
        return old

    def find(self, key):
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
