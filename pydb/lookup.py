from .core import Expr


class IndexedLookup(Expr):
    def __init__(self, seq, index, key):
        self.seq = seq
        self.index = index
        self.key = key

    def exec(self):
        rid = self.index.find(self.key)
        return (self.seq[rid]) if rid else ()
