import urllib.parse

from .core import Database
from .disk import DiskDatabase
from .mem import MemDatabase


def connect(url, *flags, **options) -> Database:
    url = urllib.parse.urlparse(url)
    if url.scheme == "mem":
        return MemDatabase(url.path)
    elif url.scheme in ("disk", ""):
        return DiskDatabase.open(url.path)
    else:
        raise ValueError("unrecognized scheme: {}".format(url.scheme))
