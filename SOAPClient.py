import csv
import os
from zeep import Client
from datetime import datetime, timedelta
from time import sleep, time
from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')

FILE = "docs/data.csv"
ENDPOINT = os.getenv('ENDPOINT')
APIKEY = os.getenv('APIKEY')
FIELDNAMES = ["Stream", "Variable", "Fecha", "id", "Sitio", "Valor"]
DAYS_DELTA = 1
MINUTES_DELTA = 10


class LatinaClient(Client):

    def __init__(self, file=FILE, days=DAYS_DELTA, minutes=MINUTES_DELTA):
        self.url = ENDPOINT
        self.apikey = APIKEY
        self.input = {
            # "siteId" : 19, # Polkura
            "initialDate": '2019-05-14 19:56:04',
            "finalDate": '2019-05-14 20:41:05'
        }
        self.file = file
        self.days_delta = days
        self.minutes_delta = minutes
        super(LatinaClient, self).__init__(self.url)

    def get_data(self):
        initial = datetime.now() + timedelta(days=-self.days_delta)
        initialDate = initial.strftime("%Y-%m-%d %H:%M:%S")
        finalDate = (initial + timedelta(minutes=self.minutes_delta)
                     ).strftime("%Y-%m-%d %H:%M:%S")

        self.input["initialDate"] = initialDate
        self.input["finalDate"] = finalDate

        data = self.service.getData(input=self.input, apikey=self.apikey)
        return self._process_data(data)

    def _process_data(self, data):
        out = []

        for d in data:
            new = {
                "Stream": "S",
                "Variable": "".join([i.capitalize() for i in d["variableName"].split(" ")]),  # noqa
                "Fecha": d["measuredTime"],
                "id": d["id"],
                "Sitio": d["siteName"],
                "Valor": d["value"],
            }
            out.append(new)
        return sorted(out, key=lambda d: d["Fecha"])

    def write(self, data):
        with open(self.file, "w") as arch:
            # Write new data
            writer = csv.DictWriter(arch, fieldnames=FIELDNAMES, delimiter=";")
            writer.writeheader()
            for d in data:
                writer.writerow(d)


if __name__ == "__main__":
    client = LatinaClient()

    while True:
        t0 = time()
        data = client.get_data()
        client.write(data)
        print("Data retrieved! Now sleeping...")
        delta = time() - t0
        sleep(client.minutes_delta * 60 - delta)
