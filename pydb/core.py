from .query import Query

from typing import Text, Union, Iterable, Tuple


class Cursor(Iterable[Tuple]):
    pass


class Database:
    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def exec(self, query: Union[Text, Query], **options) -> Cursor:
        pass

    def delete(self):
        pass


def pydb_open(url, **options) -> Database:
    pass
