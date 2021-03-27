from typing import TypeVar

T = TypeVar("T")


class Filter:
    def apply(self, record: T) -> bool:
        pass


class DynamicFilter(Filter):
    def __init__(self, fn):
        self._fn = fn

    def apply(self, record):
        return self._fn(record)
