from .query import Query

from typing import Text, Union, Iterator, Tuple


class Cursor(Iterator[Tuple]):
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


def open(url, **options) -> Database:
    pass
