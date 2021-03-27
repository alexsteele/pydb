from .query import Query


class QueryParser:
    def parse(self, raw: str) -> Query:
        pass


def parse_query(raw):
    return QueryParser().parse(raw)
