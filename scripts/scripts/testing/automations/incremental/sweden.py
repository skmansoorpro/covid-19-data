import re
from typing import Iterator

from epiweeks import Week
from bs4 import BeautifulSoup, element
import pandas as pd


from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_date, clean_count
from cowidev.testing import CountryTestBase


class Sweden(CountryTestBase):
    location = "Sweden"
    units = "tests performed"
    source_label = "Swedish Public Health Agency"
    notes = ""
    source_url_ref = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/antalet-testade-for-covid-19/"
    week_num = None
    regex = {
        "title": r"Antalet testade individer och genomförda test per",
        "week": r"[vV]ecka (\d+)",
    }

    def read(self) -> pd.DataFrame:
        """Reads data from source."""

        soup = get_soup(self.source_url_ref)
        data = self._parse_data(soup)

        return pd.DataFrame(data)

    def _parse_data(self, soup: BeautifulSoup) -> list:
        """Gets data from the source page."""
        # Extract the relevant element
        elem = self._get_relevant_element(soup)
        if not elem:
            raise TypeError("Website Structure Changed, please update the script")
        # get the week numeber
        self._get_week_num_from_element(elem)
        if self.week_num == 1:
            raise ValueError("Week 1: New Year, please update date in the script")
        # Extract the text from the element
        text = self._get_text_from_element(elem)
        # Extract the metrics
        weekly_change = self._parse_metrics(text)
        # Parse date
        week = self._parse_date()
        # Create list
        data = []
        for day in week:
            daily_change = round(weekly_change / 7.0)
            record = {
                "Date": clean_date(day),
                "Daily change in cumulative total": daily_change,
            }
            data.append(record)

        return data

    def _get_week_num_from_element(self, elem: element.Tag) -> None:
        """Gets week number from element."""
        self.week_num = int(re.search(self.regex["week"], elem.text).group(1))

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Gets element from the soup."""
        elem_list = soup.find_all("h2")
        elem = [title for title in elem_list if self.regex["title"] in title.text]
        return elem[0]

    def _get_text_from_element(self, elem: element.Tag) -> str:
        """Gets text from element."""
        text = elem.find_next_sibling("table").text.replace(" ", "").replace("\n", " ")
        return text

    def _parse_metrics(self, text: str) -> int:
        """Gets metrics from the text."""
        count = re.search(fr"[vV]ecka{self.week_num} \d+ (\d+)", text).group(1)
        return clean_count(count)

    def _parse_date(self) -> Iterator:
        """parses the date from the week number."""
        week = Week(2022, self.week_num, system="iso").iterdates()
        return week

    def export(self):
        """Exports data to csv."""
        df = self.read().pipe(self.pipe_metadata)
        self.export_datafile(df, attach=True)


def main():
    Sweden().export()


if __name__ == "__main__":
    main()
