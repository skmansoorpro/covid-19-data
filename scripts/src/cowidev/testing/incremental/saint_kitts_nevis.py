import os
from cowidev.testing.utils.base import CountryTestBase

import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow


class SaintKittsNevis(CountryTestBase):
    location = "Saint Kitts and Nevis"

    def export(self):
        url = "https://covid19.gov.kn/src/stats2/"

        soup = get_soup(url)
        df = pd.read_html(str(soup.find("table")))[0]
        count = df.loc[df[0] == "No. of Persons Tested", 1].item()
        # print(count)

        date_str = localdatenow("America/St_Kitts")
        df = pd.DataFrame(
            {
                "Country": self.location,
                "Date": [date_str],
                "Cumulative total": count,
                "Source URL": url,
                "Source label": "Ministry of Health",
                "Units": "people tested",
                "Notes": pd.NA,
            }
        )

        if os.path.isfile(self.output_path):
            existing = pd.read_csv(self.output_path)
            if count > existing["Cumulative total"].max() and date_str > existing["Date"].max():
                df = pd.concat([df, existing]).sort_values("Date", ascending=False).drop_duplicates()
                df.to_csv(self.output_path, index=False)


def main():
    SaintKittsNevis().export()
