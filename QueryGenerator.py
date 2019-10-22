from .SOAPClient import LatinaClient, FIELDNAMES
import csv
from numpy import histogram, random, where

DAYS_DELTA = 7
MINUTES_DELTA = DAYS_DELTA * 24 * 60
DATA_FILE = "docs/data_pool.csv"
OUT_FILE = "docs/query.txt"
DESC_FILE = "docs/StreamDescription.txt"
FETCH = False
NUMBER_BINS = 10
NUMBER_QUERIES = 100
MAX_VARS = 20
MIN_VARS = 2

FILTER_TEMPLATE = "( {0}[value >= {1}] AND {0}[value <= {2}] )"
QUERY_TEMPLATE = "SELECT * FROM S WHERE ( {} ) FILTER ( {} );"
EVENT_DECLARE_TEMPLATE = "DECLARE EVENT {}(id long, sitio string, value double)\n"
STREAM_DECLARE_TEMPLATE = "DECLARE STREAM S({})\n"

BLACKLIST = {"VolumetricWaterContent(30cm)",
             "VolumetricWaterContent(60cm)",
             "VolumetricWaterContent(90cm)"}


class QueryGenerator:
    def __init__(self, file=OUT_FILE):
        provider = QueryDataProvider()
        self.histograms = provider.get_histograms()
        self.event_types = list(self.histograms.keys())
        self.file = file

    def _get_query_events_number(self):
        n = len(self.event_types)
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


class Query:
    def __init__(self, events, filters):
        self.events = events
        self.filters = filters

    def __str__(self):
        events_str = " ; ".join(self.events)
        filters_str = " AND ".join((str(f) for f in self.filters))
        return QUERY_TEMPLATE.format(events_str, filters_str)


class QueryFilter:
    def __init__(self, event, low, hi):
        self.event = event
        self.low = low
        self.hi = hi

    def __str__(self):
        return FILTER_TEMPLATE.format(self.event, self.low, self.hi)


class QueryDataProvider:
    def __init__(self, file=DATA_FILE, days=DAYS_DELTA, minutes=MINUTES_DELTA):
        self.file = file
        self.days = days
        self.minutes = minutes

    def _get_data(self):
        data = []

        if FETCH:
            client = LatinaClient(
                file=self.file, days=self.days, minutes=self.minutes)
            data = client.get_data()
            client.write(data)

        else:
            with open(self.file, newline='') as arch:
                arch.readline()  # skip headers
                reader = csv.DictReader(
                    arch, fieldnames=FIELDNAMES, delimiter=";")
                for row in reader:
                    data.append(row)

        return data

    def _var_to_val(self, data):
        out = {i["Variable"]: [] for i in data}
        for d in data:
            out[d["Variable"]].append(float(d["Valor"]))

        return out

    def _build_histograms(self, var_val):
        histograms = {}

        for var, vals in var_val.items():
            if var in BLACKLIST:
                continue
            histograms[var] = histogram(vals, bins=NUMBER_BINS)

        return histograms

    def _filter_histograms(self, histograms):
        filtered_histograms = dict()
        for var in histograms:
            bins, ranges = histograms[var]
            filtered_bins, filtered_ranges = [], []
            for i, bin in enumerate(bins):
                if bin > 0:
                    filtered_bins.append(bin)
                    filtered_ranges.append(ranges[i])
            filtered_ranges.append(ranges[-1])
            filtered_histograms[var] = (filtered_bins, filtered_ranges)

        return filtered_histograms

    def get_histograms(self):
        data = self._get_data()
        var_val = self._var_to_val(data)
        histograms = self._build_histograms(var_val)
        #filtered_histograms = self._filter_histograms(histograms)
        return histograms


if __name__ == "__main__":
    qg = QueryGenerator()
    qg.generate_queries()
    qg.generate_stream_description()
