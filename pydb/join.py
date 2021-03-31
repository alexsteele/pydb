from enum import Enum
from typing import Generic, Iterator, Optional, Tuple, TypeVar

from .expr import Expr

A = TypeVar("A")
B = TypeVar("B")


class JoinKind(Enum):
    INNER = "INNER"
    LEFT_OUTER = "LEFT_OUTER"
    RIGHT_OUTER = "RIGHT_OUTER"
    FULL_OUTER = "FULL_OUTER"


class Join(Expr):
    pass


class InnerJoin(Join, Generic[A, B]):
    def exec(self) -> Iterator[Tuple[A, B]]:
        pass


class LeftOuterJoin(Join, Generic[A, B]):
    def exec(self) -> Iterator[Tuple[A, Optional[B]]]:
        pass


class RightOuterJoin(Join, Generic[A, B]):
    def exec(self) -> Iterator[Tuple[Optional[A], B]]:
        pass


class FullOuterJoin(Join, Generic[A, B]):
    def exec(self) -> Iterator[Tuple[Optional[A], Optional[B]]]:
        pass


class NestedLoopJoin(InnerJoin):
    def __init__(self, exp1, keyfn1, exp2, keyfn2):
        self.exp1
        self.keyfn1 = keyfn1
        self.exp2 = exp2
        self.keyfn2 = keyfn2

    def exec(self):
        for a in self.exp1.exec():
            for b in self.exp2.exec():
                if self.keyfn1(a) == self.keyfn2(b):
                    yield (a, b)


class HashJoin(InnerJoin):
    def __init__(self, exp1, keyfn1, exp2, keyfn2):
        self.exp1
        self.keyfn1 = keyfn1
        self.exp2 = exp2
        self.keyfn2 = keyfn2

    def exec(self):
        sentinel = object()
        index = {self.keyfn1(a): a for a in self.exp1.exec()}
        for b in self.exp2.exec():
            a = index.get(keyfn2(b), sentinel)
            if a is not sentinel:
                yield (a, b)


class MergeJoin(InnerJoin):
    def __init__(self, exp1, keyfn1, exp2, keyfn2):
        self.exp1
        self.keyfn1 = keyfn1
        self.exp2 = exp2
        self.keyfn2 = keyfn2

    def exec(self):
        sentinel = object()
        it1 = self.exp1.exec()
        it2 = self.exp2.exec()
        a = next(it1, sentinel)
        b = next(it2, sentinel)
        while sentinel not in (a, b):
            k1 = self.keyfn1(a)
            k2 = self.keyfn2(b)
            if k1 == k2:
                yield (a, b)
                a = next(it1, sentinel)
                b = next(it2, sentinel)
            elif k1 < k2:
                a = next(it1, sentinel)
            else:
                b = next(it2, sentinel)
