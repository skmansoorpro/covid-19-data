import os
from cowidev.testing.utils.base import CountryTestBase
import pandas as pd

from cowidev.utils import get_soup, clean_count
from cowidev.utils.clean.dates import localdate


class Azerbaijan(CountryTestBase):
    location = "Azerbaijan"

    def export(self):
        data = pd.read_csv(self.output_path).sort_values(by="Date", ascending=False)

        source_url = "https://koronavirusinfo.az/az/page/statistika/azerbaycanda-cari-veziyyet"

        soup = get_soup(source_url)

        element = soup.find_all("div", class_="gray_little_statistic")[5].find("strong")
        cumulative_total = clean_count(element.text)

        if cumulative_total > data["Cumulative total"].max():
            new = pd.DataFrame(
                {
                    "Cumulative total": cumulative_total,
                    "Date": [localdate("Asia/Baku")],
                    "Country": self.location,
                    "Units": "tests performed",
                    "Source URL": source_url,
                    "Source label": "Cabinet of Ministers of Azerbaijan",
                }
            )

            df = pd.concat([new, data], sort=False)
            self.export_datafile(df)


def main():
    Azerbaijan().export()
