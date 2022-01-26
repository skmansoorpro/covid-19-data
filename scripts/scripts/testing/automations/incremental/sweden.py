import re
from datetime import timedelta

from epiweeks import Week
from bs4 import BeautifulSoup, element
import pandas as pd

from cowidev.utils import get_soup, clean_count, clean_date
from cowidev.testing.utils.base import CountryTestBase


class Sweden(CountryTestBase):
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
        df = self._build_df(data)
        return df

    def _build_df(self, data: dict):
        # Create dataframe
        df = pd.DataFrame([data])
        # Create date range (check week distance)
        dt_min = self._load_last_date() + timedelta(days=1)
        dt = clean_date(df.Date.max(), "%Y-%m-%d", as_datetime=True)
        if ((days_diff := (dt - dt_min).days) != 6) & (days_diff != -1):
            raise ValueError(f"Date distance is no longer a week ({days_diff})! Please check.")
        ds = pd.Series(pd.date_range(dt - timedelta(days=6), dt).astype(str), name="Date")
        # Distribute week value over 7 days
        df = df.merge(ds, how="right")
        df = df.assign(
            **{
                "Source URL": self.source_url,
                "Daily change in cumulative total": round(df["Daily change in cumulative total"].bfill() / 7),
            }
        )
        return df

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """Gets data from the source page."""
        # Extract the relevant element
        elem = self._get_relevant_element(soup)
        # Get the week number
        week_num = self._get_week_num_from_element(elem)
        # Extract the text from the element
        text = self._get_text_from_element(elem)
        # Extract the metrics
        daily_change = self._parse_metrics(text, week_num)
        # Parse date
        date = self._parse_date(week_num)
        # Calculate the cumulative total
        # count = self._calc_cumulative_total(daily_change, date)   #if needed

        record = {
            "Date": date,
            "Daily change in cumulative total": daily_change,
            # "count": count,                       #if needed
        }
        return record

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Gets element from the soup."""
        elem_list = soup.find_all("h2")
        elem = [title for title in elem_list if self.regex["title"] in title.text]
        elem = elem[0]
        if not elem:
            raise TypeError("Website Structure Changed, please update the script")
        return elem

    def _get_week_num_from_element(self, elem: element.Tag) -> None:
        week_num = int(re.search(self.regex["week"], elem.text).group(1))
        if self.week_num == 1:
            raise ValueError("Week 1: New Year, please update date in the script")
        return week_num

    def _get_text_from_element(self, elem: element.Tag) -> str:
        """Gets text from element."""
        elem = elem.find_next_sibling("table")
        return elem.text.replace(" ", "").replace("\n", " ")

    def _parse_metrics(self, text: str, week_num) -> int:
        """Gets metrics from the text."""
        count = re.search(fr"[vV]ecka{week_num} \d+ (\d+)", text).group(1)
        return clean_count(count)

    def _parse_date(self, week_num) -> str:
        """parses the date from the week number."""
        date = Week(2022, week_num, system="iso").enddate()
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

    def _load_last_date(self):
        df_current = self.load_datafile()
        date = df_current.Date.max()
        return clean_date(date, "%Y-%m-%d", as_datetime=True)

    def export(self):
        """Exports data to csv."""
        df = self.read().pipe(self.pipe_metadata)
        self.export_datafile(df, attach=True)


def main():
    Sweden().export()


if __name__ == "__main__":
    main()
