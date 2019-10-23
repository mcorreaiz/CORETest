from numpy import random, where
from Query import Query, QueryFilter, Clause, EventClause, Event
from QueryDataProvider import QueryDataProvider

OUT_FILE = "docs/query.txt"
DESC_FILE = "docs/StreamDescription.txt"
NUM_QUERIES = 10
NUM_EVENTS = (1, 10)
NUM_FL_CLAUSE = (1, 10)
NUM_SL_CLAUSE = (2, 20)
FILTER_PROB = 3 / 4

EVENT_DECLARE_TEMPLATE = "DECLARE EVENT {}(id long, sitio string, value double)\n"  # noqa
STREAM_DECLARE_TEMPLATE = "DECLARE STREAM S({})\n"


class QueryFactory:
    def __init__(self, file=OUT_FILE):
        provider = QueryDataProvider()
        self.histograms = provider.get_histograms()
        self.event_types = list(self.histograms.keys())
        self.file = file
        self.CF = ClauseFactory(self.event_types)

    def build_queries(self, n=NUM_QUERIES):
        queries = [str(self._build_query()) for _ in range(n)]
        print(queries)
        self._write_queries(queries)

    def build_stream_description(self):
        events_str = ", ".join(self.event_types)
        with open(DESC_FILE, "w") as arch:
            for ev in self.event_types:
                arch.write(EVENT_DECLARE_TEMPLATE.format(ev))
            arch.write(STREAM_DECLARE_TEMPLATE.format(events_str))

    def _get_query_clauses(self):
        clauses = self.CF.build_clauses()
        return clauses

    def _get_bin(self, histogram):
        num_obs = sum(histogram[0])
        observations, bins = histogram
        probs = [obs / num_obs for obs in observations]
        bin = random.choice(observations, p=probs)
        index, = where(observations == bin)
        low, hi = bins[index][0], bins[index+1][0]
        return low, hi

    def _build_query_filter(self, event):
        bins = self.histograms[event.type]
        low, hi = self._get_bin(bins)
        query_filter = QueryFilter(event, low, hi)
        return query_filter

    def _build_query(self):
        query_clauses = self._get_query_clauses()
        # Determine which aliases will have filter, then create filters
        query_filters = [self._build_query_filter(
            ev) for ev in self.CF.events]
        query = Query(query_clauses, query_filters)
        return query

    def _write_queries(self, queries):
        with open(self.file, "w") as arch:
            arch.write("\n".join(queries))


class ClauseFactory:
    def __init__(self, event_types):
        self.event_types = event_types
        self.event_counter = {ev: 0 for ev in self.event_types}
        self.events = []

    def build_clauses(self):
        self._reset()
        # fl = first level, sl = second level
        num_fl_clauses = self._get_clause_number(*NUM_FL_CLAUSE, 2)
        fl_clauses = []
        for _ in range(num_fl_clauses):
            num_sl_clauses = self._get_clause_number(*NUM_SL_CLAUSE, 1)
            sl_clauses = []
            for _ in range(num_sl_clauses):
                clause_events = self._get_clause_events()
                sl_clauses.append(EventClause(clause_events, "OR"))
            fl_clauses.append(Clause(sl_clauses, ";"))

        return fl_clauses

    def _reset(self):
        self.event_counter = {ev: 0 for ev in self.event_types}
        self.events = []

    def _get_clause_events(self):
        num_events = self._get_clause_number(*NUM_EVENTS, 1)
        clause_event_types = random.choice(self.event_types, num_events,
                                           replace=False)
        clause_events = self._build_clause_events(clause_event_types)
        return clause_events

    def _build_clause_events(self, event_types):
        clause_events = []
        for ev in event_types:
            self.event_counter[ev] += 1
            event = Event(ev, self.event_counter[ev])
            clause_events.append(event)
        self.events.extend(clause_events)
        return clause_events

    def _get_clause_number(self, m, M, k):
        k = random.randint(m, M+1)
        random.zipf
        return k


if __name__ == "__main__":
    qf = QueryFactory()
    qf.build_queries()
    qf.build_stream_description()
