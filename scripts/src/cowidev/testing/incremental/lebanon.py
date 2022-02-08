from cowidev.testing.utils.base import CountryTestBase

import pandas as pd

from cowidev.utils import clean_count, get_soup
from cowidev.utils.clean import extract_clean_date


class Lebanon(CountryTestBase):
    location = "Lebanon"

    def export(self):
        data = pd.read_csv(self.output_path).sort_values(by="Date", ascending=False)

        source_url = "https://corona.ministryinfo.gov.lb/"

        soup = get_soup(source_url)

        element = soup.find("h1", class_="s-counter3")
        cumulative_total = clean_count(element.text)

        date_raw = soup.select(".last-update strong")[0].text
        date = extract_clean_date(date_raw, regex=r"([A-Za-z]+ \d+)", date_format="%b %d", replace_year=2021)

        if cumulative_total > data["Cumulative total"].max():
            new = pd.DataFrame(
                {
                    "Cumulative total": cumulative_total,
                    "Date": [date],
                    "Country": self.location,
                    "Units": "tests performed",
                    "Source URL": source_url,
                    "Source label": "Lebanon Ministry of Health",
                }
            )

            df = pd.concat([new, data], sort=False)
            self.export_datafile(df)


def main():
    Lebanon().export()
