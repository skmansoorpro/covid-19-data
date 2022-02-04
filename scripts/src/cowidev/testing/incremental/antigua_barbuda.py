import os
import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow
from cowidev.testing.utils.base import CountryTestBase


class AntiguaBarbuda(CountryTestBase):
    location = "Antigua and Barbuda"

    def export(self):
        url = "https://covid19.gov.ag"
        location = "Antigua and Barbuda"
        soup = get_soup(url)

        stats = soup.find_all("p", attrs={"class": "case-Number"})
        count = int(stats[3].text)
        # print(count)

        date_str = localdatenow("America/St_Johns")
        df = pd.DataFrame(
            {
                "Country": location,
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
    AntiguaBarbuda().export()
