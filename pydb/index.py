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

    def remove(self, key: K) -> Optional[V]:
        pass


class SortedIndex(Index):
    def find(self, key):
        return next(self.scan(key), None)

    def scan(self, start: K = None) -> Iterator[V]:
        pass

    def rscan(self, start: K = None) -> Iterator[V]:
        pass


class HashIndex(Index):
    def __init__(self, index: Dict[K, V] = None):
        self._index = index or {}

    def find(self, key):
        return self._index.get(key, None)

    def insert(self, key, val):
        if key in self._index:
            raise ValueError("duplicate key {}".format(key))
        self._index[key] = val

    def update(self, key, val):
        old = self._index.get(key, None)
        self._index[key] = val
        return old

    def remove(self, key):
        val = self._index.get(key)
        if val:
            del self._index[key]
        return val

class SortedListIndex(SortedIndex):
    def __init__(self):
        self._keys = []
        self._vals = []

    def insert(self, key, val):
        idx = bisect.bisect_left(self._keys, key)
        self._keys.insert(idx, key)
        self._vals.insert(idx, val)

    def update(self, key, val):
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            old = self._vals[idx]
            self._vals[idx] = val
            return old
        return None

    def scan(self, key = None):
        if key is None:
            return iter(self._vals)
        idx = bisect.bisect_left(self._keys, key)
        return iter(self._vals[idx:])

    def rscan(self, key = None):
        if key is None:
            return reversed(self._vals)
        idx = bisect.bisect_right(self._keys, key)
        return reversed(self._vals[:idx])

    def remove(self, key):
        idx = bisect.bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            self._keys.pop(idx)
            return self._vals.pop(idx)
        return None
