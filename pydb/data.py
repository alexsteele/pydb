from typing import (
    Callable,
    TypeVar,
    Optional,
    Iterator,
    Iterable,
)

K = TypeVar("K")
V = TypeVar("V")

QueryFunction = Callable[[K], Optional[V]]

BatchQueryFunction = Callable[[Iterable[K]], Iterator[V]]

ScanFunction = Callable[[K], Iterator[V]]
