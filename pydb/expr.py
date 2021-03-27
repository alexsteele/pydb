from dataclasses import dataclass
from typing import (
    Iterator,
    TypeVar,
    Generic,
    Tuple,
    Sequence,
)


class Expr:
    def exec(self) -> Iterator[Tuple]:
        pass


@dataclass
class Seq(Expr):
    seq: Sequence[Tuple]

    def exec(self) -> Iterator[Tuple]:
        return iter(seq)
