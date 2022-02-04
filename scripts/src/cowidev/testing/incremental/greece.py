import re
from cowidev.testing.utils.base import CountryTestBase

import pandas as pd

from cowidev.utils import get_soup, clean_count


class Greece(CountryTestBase):
    location = "Greece"

    def export(self):

        data = pd.read_csv(self.output_path)

        url = "https://covid19.gov.gr/"
        soup = get_soup(url)

        count = clean_count(
            soup.select(".elementor-element-9df72a6 .elementor-size-default")[0].text.replace("ΣΥΝΟΛΟ: ", "")
        )

        date_str = re.search(r"Τελευταία ενημέρωση\: ([\d/]{,10})", soup.text).group(1)
        date_str = str(pd.to_datetime(date_str, dayfirst=True).date())

        if count > data["Cumulative total"].max() and date_str > data["Date"].max():

            new = pd.DataFrame(
                {
                    "Country": self.location,
                    "Date": [date_str],
                    "Cumulative total": count,
                    "Source URL": url,
                    "Source label": "National Organization of Public Health",
                    "Units": "samples tested",
                }
            )

            df = pd.concat([new, data], sort=False).sort_values("Date")
            self.export_datafile(df)


def main():
    Greece().export()
