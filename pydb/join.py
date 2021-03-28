from enum import Enum
from .core import Expr
from typing import (
    Iterator,
    Iterable,
    TypeVar,
    Tuple,
    Callable,
    Optional,
)

A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


class JoinType(Enum):
    INNER = "INNER"
    LEFT_OUTER = "LEFT_OUTER"
    RIGHT_OUTER = "RIGHT_OUTER"
    FULL_OUTER = "FULL_OUTER"


class Join(Expr):
    pass


class InnerJoin(Join):
    def join(
        self,
        r1: Iterable[A],
        f1: Callable[[A], C],
        r2: Iterable[B],
        f2: Callable[[B], C],
    ) -> Iterator[Tuple[A, B]]:
        pass


class LeftOuterJoin(Join):
    def join(
        self,
        r1: Iterable[A],
        f1: Callable[[A], C],
        r2: Iterable[B],
        f2: Callable[[B], C],
    ) -> Iterator[Tuple[A, Optional[B]]]:
        pass


class RightOuterJoin(Join):
    def join(
        self,
        r1: Iterable[A],
        f1: Callable[[A], C],
        r2: Iterable[B],
        f2: Callable[[B], C],
    ) -> Iterator[Tuple[Optional[A], B]]:
        pass


class FullOuterJoin(Join):
    def join(
        self,
        r1: Iterable[A],
        f1: Callable[[A], C],
        r2: Iterable[B],
        f2: Callable[[B], C],
    ) -> Iterator[Tuple[Optional[A], Optional[B]]]:
        pass


class NestedLoopJoin(InnerJoin):
    def __init__(self, seq1, f1, seq2, f2):
        self.seq1
        self.f1 = f1
        self.seq2 = seq2
        self.f2 = f2

    def exec(self) -> Iterator[Tuple[A, B]]:
        for a in self.seq1:
            for b in self.seq2:
                if f1(a) == f2(b):
                    yield (a, b)


class HashJoin(InnerJoin):
    def __init__(self, seq1, f1, seq2, f2):
        self.seq1
        self.f1 = f1
        self.seq2 = seq2
        self.f2 = f2

    def exec(self):
        sentinel = object()
        index = {self.f1(a): a for a in self.seq1}
        for b in self.seq2:
            a = index.get(f2(b), sentinel)
            if a is not sentinel:
                yield (a, b)


class MergeJoin(InnerJoin):
    def __init__(self, seq1, f1, seq2, f2):
        self.seq1
        self.f1 = f1
        self.seq2 = seq2
        self.f2 = f2

    def exec(self):
        sentinel = object()
        it1 = iter(self.seq1)
        it2 = iter(self.seq2)
        a = next(it1, sentinel)
        b = next(it2, sentinel)
        while sentinel not in (a, b):
            k1 = self.f1(a)
            k2 = self.f2(b)
            if k1 == k2:
                yield (a, b)
                a = next(it1, sentinel)
                b = next(it2, sentinel)
            elif k1 < k2:
                a = next(it1, sentinel)
            else:
                b = next(it2, sentinel)
