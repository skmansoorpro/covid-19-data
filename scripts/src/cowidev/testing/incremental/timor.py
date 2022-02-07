import os
import re
from cowidev.testing.utils.base import CountryTestBase

import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow


class TimorLeste(CountryTestBase):
    location = "Timor"

    def export(self):
        url = "https://covid19.gov.tl/dashboard/"
        soup = get_soup(url)

        stats = soup.select("#testing .c-green .wdt-column-sum")[0].text
        count = int("".join(re.findall("[0-9]", stats)))
        # print(count)

        date_str = localdatenow("Asia/Dili")
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
                df.to_csv(self.output_path, index=False)


def main():
    TimorLeste().export()
