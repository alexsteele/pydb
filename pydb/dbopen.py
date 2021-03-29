import urllib.parse

from .core import Database
from .mem import MemDatabase


def pydb_open(url, *flags, **options) -> Database:
    url = urllib.parse.urlparse(url)
    if url.scheme == "mem":
        return MemDatabase(url.path)
    else:
        raise ValueError("unrecognized scheme: {}".format(url.scheme))
