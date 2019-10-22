from numpy import random, where
from Query import Query, QueryFilter
from QueryDataProvider import QueryDataProvider

OUT_FILE = "docs/query.txt"
DESC_FILE = "docs/StreamDescription.txt"
NUMBER_QUERIES = 100
MAX_VARS = 20
MIN_VARS = 2
FILTER_PROB = 3 / 4

EVENT_DECLARE_TEMPLATE = "DECLARE EVENT {}(id long, sitio string, value double)\n"  # noqa
STREAM_DECLARE_TEMPLATE = "DECLARE STREAM S({})\n"


class QueryGenerator:
    def __init__(self, file=OUT_FILE):
        provider = QueryDataProvider()
        self.histograms = provider.get_histograms()
        self.event_types = list(self.histograms.keys())
        self.file = file

    def _get_query_events_number(self):
        k = random.randint(MIN_VARS, MAX_VARS + 1)
        return k

    def _get_query_events(self):
        k = self._get_query_events_number()
        query_events = random.choice(self.event_types, k, replace=False)
        return query_events

    def _get_bin(self, histogram):
        num_obs = sum(histogram[0])
        observations, bins = histogram
        probs = [obs / num_obs for obs in observations]
        bin = random.choice(observations, p=probs)
        index, = where(observations == bin)
        low, hi = bins[index][0], bins[index+1][0]
        return low, hi

    def _build_query_filter(self, event):
        bins = self.histograms[event]
        low, hi = self._get_bin(bins)
        query_filter = QueryFilter(event, low, hi)
        return query_filter

    def _build_query(self):
        query_events = self._get_query_events()
        query_filters = [self._build_query_filter(
            event) for event in query_events]
        query = Query(query_events, query_filters)
        return query

    def _write_queries(self, queries):
        with open(self.file, "w") as arch:
            arch.write("\n".join(queries))

    def generate_queries(self, n=NUMBER_QUERIES):
        queries = [str(self._build_query()) for _ in range(n)]
        self._write_queries(queries)

    def generate_stream_description(self):
        events_str = ", ".join(self.event_types)
        with open(DESC_FILE, "w") as arch:
            for ev in self.event_types:
                arch.write(EVENT_DECLARE_TEMPLATE.format(ev))
            arch.write(STREAM_DECLARE_TEMPLATE.format(events_str))


if __name__ == "__main__":
    qg = QueryGenerator()
    qg.generate_queries()
    qg.generate_stream_description()
