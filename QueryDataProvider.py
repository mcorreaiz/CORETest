import csv
from SOAPClient import LatinaClient, FIELDNAMES
from numpy import histogram

FETCH = False
NUMBER_BINS = 10
DAYS_DELTA = 7
MINUTES_DELTA = DAYS_DELTA * 24 * 60
DATA_FILE = "docs/data_pool.csv"

BLACKLIST = {"VolumetricWaterContent(30cm)",
             "VolumetricWaterContent(60cm)",
             "VolumetricWaterContent(90cm)"}


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
        # filtered_histograms = self._filter_histograms(histograms)
        return histograms
