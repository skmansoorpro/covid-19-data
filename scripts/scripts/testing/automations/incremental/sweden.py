import os
import re

from epiweeks import Week
from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils import paths
from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class Sweden:
    location = "Sweden"
    units = "tests performed"
    source_label = "Swedish Public Health Agency"
    notes = ""
    source_url = "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/antalet-testade-for-covid-19/"
    week_num = None
    regex = {
        "title": r"Antalet testade individer och genomförda test per",
        "week": r"[vV]ecka (\d+)",
    }

    def read(self) -> pd.Series:
        """Reads data from source."""

        soup = get_soup(self.source_url)
        data = self._parse_data(soup)

        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
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
        daily_change = self._parse_metrics(text)
        # Parse date
        date = self._parse_date()
        # Calculate the cumulative total
        # count = self._calc_cumulative_total(daily_change, date)   #if needed

        record = {
            "source_url": self.source_url,
            "date": date,
            "daily_change": daily_change,
            # "count": count,                       #if needed
        }
        return record

    def _get_week_num_from_element(self, elem: element.Tag) -> None:
        self.week_num = int(re.search(self.regex["week"], elem.text).group(1))

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Gets element from the soup."""
        elem_list = soup.find_all("h2")
        elem = [title for title in elem_list if self.regex["title"] in title.text]
        return elem[0]

    def _get_text_from_element(self, elem: element.Tag) -> str:
        """Gets text from element."""
        elem = elem.find_next_sibling("table")
        return elem.text.replace(" ", "").replace("\n", " ")

    def _parse_metrics(self, text: str) -> int:
        """Gets metrics from the text."""
        count = re.search(fr"[vV]ecka{self.week_num} \d+ (\d+)", text).group(1)
        return clean_count(count)

    def _parse_date(self) -> str:
        """parses the date from the week number."""
        date = Week(2022, self.week_num, system="iso").enddate()
        return clean_date(date)

    # IF NEEDED

    # def _calc_cumulative_total(self, daily_change: int, date: str) -> int:
    #     """Calculates the cumulative total."""
    #     output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{self.location}.csv")
    #     _df = pd.read_csv(output_path).sort_values("Date")
    #     if _df["Date"].iloc[-1] == date:
    #         return _df["Cumulative total"].iloc[-1]
    #     count = _df["Cumulative total"].iloc[-1] + daily_change
    #     return clean_count(count)

    def export(self):
        """Exports data to csv."""
        data = self.read()
        increment(
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=data["source_url"],
            source_label=self.source_label,
            daily_change=data["daily_change"],
            # count=data["count"],                        #if needed
        )


def main():
    Sweden().export()


if __name__ == "__main__":
    main()
