from . import context
import unittest
import random
from pydb.index import (
    Index,
    SortedIndex,
    HashIndex,
    SortedListIndex,
)


def test_hash_index():
    check_index_ops(HashIndex())


def test_sorted_index():
    check_index_ops(SortedListIndex())
    check_sorted_index_ops(SortedListIndex())


def check_index_ops(index: Index):
    assert index.find("foo") == None
    index.insert("foo", "bar")
    assert index.find("foo") == "bar"
    index.update("foo", "baz")
    assert index.find("foo") == "baz"
    index.remove("foo")
    assert index.find("foo") == None


def check_sorted_index_ops(index: SortedIndex):
    items = [(x, str(x + 1)) for x in range(10)]
    shuffled = list(items)
    random.shuffle(shuffled)
    for key, val in shuffled:
        index.insert(key, val)

    def vals(items):
        return [item[1] for item in items]

    def remove(*indexes):
        for idx in indexes:
            index.remove(items[idx][0])
            items.pop(idx)

    def check():
        assert list(index.scan()) == vals(items)
        assert list(index.rscan()) == list(reversed(vals(items)))
        for idx in range(len(items)):
            key = items[idx][0]
            assert list(index.scan(key)) == vals(items[idx:])
            assert list(index.rscan(key)) == list(reversed(vals(items[: idx + 1])))

    check()
    remove(8, 5, 1)
    check()
