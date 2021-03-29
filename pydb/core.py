from typing import Iterable, Text, Tuple, Union

from .query import Query


class Cursor(Iterable[Tuple]):
    pass


class Database:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def exec(self, query: Union[Text, Query], **options) -> Cursor:
        pass

    def delete(self):
        pass
