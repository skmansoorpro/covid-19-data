import json

import pandas as pd

from cowidev.utils import get_soup, clean_date
from cowidev.testing.utils.base import CountryTestBase


class Tunisia(CountryTestBase):
    location = "Tunisia"

    def export(self):
        data = pd.read_csv(self.output_path).sort_values(by="Date", ascending=False)

        source_url = "https://onmne.tn"

        soup = get_soup(source_url, verify=False)

        cumulative_total = json.loads(soup.find("span", class_="vcex-milestone-time").attrs["data-options"])["endVal"]

        date = soup.select("p span")[0].text.replace("Chiffres clés mis à jour le ", "")
        Date = clean_date(date, "%d %B %Y", lang="fr")  # .strftime("%Y-%m-%d")

        if cumulative_total > data["Cumulative total"].max():
            new = pd.DataFrame(
                {
                    "Cumulative total": cumulative_total,
                    "Date": [Date],
                    "Country": self.location,
                    "Units": "people tested",
                    "Source URL": source_url,
                    "Source label": "Tunisia Ministry of Health",
                }
            )

            df = pd.concat([new, data], sort=False)
            self.export_datafile(df)


def main():
    Tunisia().export()
