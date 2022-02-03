import os

import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow
from cowidev.testing.utils.base import CountryTestBase


class NorthMacedonia(CountryTestBase):
    location = "North Macedonia"

    def export(self):
        url = "https://koronavirus.gov.mk/"

        soup = get_soup(url)
        count = int(soup.find_all("td")[7].text.replace(",", ""))
        # print(count)

        date_str = localdatenow("Europe/Skopje")
        df = pd.DataFrame(
            {
                "Country": self.location,
                "Date": [date_str],
                "Cumulative total": count,
                "Source URL": url,
                "Source label": "Ministry of Health",
                "Units": "tests performed",
                "Notes": pd.NA,
            }
        )

        if os.path.isfile(self.output_path):
            existing = pd.read_csv(self.output_path)
            if count > existing["Cumulative total"].max() and date_str > existing["Date"].max():
                df = pd.concat([df, existing]).sort_values("Date", ascending=False).drop_duplicates()
                self.export_datafile(df)


def main():
    NorthMacedonia().export()
