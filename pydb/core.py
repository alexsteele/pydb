from typing import Iterable, Protocol, Text, Tuple, Union

from .query import Query


class Cursor(Protocol, Iterable[Tuple]):
    pass


class Database(Protocol):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def exec(self, query: Union[Text, Query], **options) -> Cursor:
        pass

    def delete(self):
        pass

    def close(self):
        pass
