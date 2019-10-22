from numpy import random

KLEENE_PROB = 1 / 20
QUERY_TEMPLATE = "SELECT * FROM S WHERE ( {} ) FILTER ( {} );"
FILTER_TEMPLATE = "( {0}[value >= {1}] AND {0}[value <= {2}] )"


class Query:
    def __init__(self, events, filters):
        self.events = events
        self.filters = filters

    def _sequence_query(self):
        return " ; ".join(self.events)

    def _or_query(self):
        return " OR ".join(self.events)

    def _add_kleene(self, query):
        kleene = random() < KLEENE_PROB
        return query + ("+" if kleene else "")

    def __str__(self):
        events_str = self._sequence_query()
        filters_str = " AND ".join((str(f) for f in self.filters))
        return QUERY_TEMPLATE.format(events_str, filters_str)


class QueryFilter:
    def __init__(self, event, low, hi):
        self.event = event
        self.low = low
        self.hi = hi

    def __str__(self):
        return FILTER_TEMPLATE.format(self.event, self.low, self.hi)
