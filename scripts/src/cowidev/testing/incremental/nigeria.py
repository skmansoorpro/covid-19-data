import os
from cowidev.testing.utils.base import CountryTestBase

import pandas as pd

from cowidev.utils import get_soup, clean_count
from cowidev.utils.utils import get_project_dir
from cowidev.utils.clean.dates import localdate


class Nigeria(CountryTestBase):
    location = "Nigeria"

    def export(self):
        data = pd.read_csv(self.output_path).sort_values(by="Date", ascending=False)

        source_url = "http://covid19.ncdc.gov.ng/"

        soup = get_soup(source_url)

        element = soup.find("div", class_="col-xl-3").find("span")
        cumulative_total = clean_count(element.text)

        if cumulative_total > data["Cumulative total"].max():

            new = pd.DataFrame(
                {
                    "Date": [localdate("Africa/Lagos")],
                    "Cumulative total": cumulative_total,
                    "Country": self.location,
                    "Units": "samples tested",
                    "Source URL": source_url,
                    "Source label": "Nigeria Centre for Disease Control",
                }
            )

            df = pd.concat([new, data], sort=False)
            self.export_datafile(df)


def main():
    Nigeria().export()
