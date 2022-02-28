import re
import tempfile


import pandas as pd
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text


from cowidev.utils import clean_count, get_soup
from cowidev.utils.clean import extract_clean_date
from cowidev.utils.utils import download_file_from_url
from cowidev.testing.utils.base import CountryTestBase


class Tanzania(CountryTestBase):
    location: str = "Tanzania"
    units: str = "tests performed"
    source_url: dict = {
        "base": "http://www.moh.go.tz",
        "bulletin": "http://www.moh.go.tz/en/covid-19-info",
    }
    source_url_ref: str = None
    source_label: str = "Ministry of Health"
    regex: dict = {
        "title": r"SITUATION REPORT",
        "date": r"(\d{1,2} \w+ 20\d{2})",
        "metrics": r"Cumulative total of (\d+) laboratory tests",
    }

    def read(self) -> pd.DataFrame:
        """Reads data from source."""
        soup = get_soup(self.source_url["bulletin"])
        df = self._parse_data(soup)
        return df

    def _parse_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parses data from link."""
        # Obtain pdf url
        href = href = soup.find("a", text=re.compile(r"SITUATION REPORT"))["href"]
        self.source_url_ref = "{}{}".format(self.source_url["base"], href)
        # Extract text from pdf url
        text = self._extract_text_from_url()
        # Clean data
        df = self._parse_metrics(text)
        # Parse date
        df = self._parse_date(text, df)
        return df

    def _extract_text_from_url(self) -> str:
        """Extracts text from pdf."""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(self.source_url_ref, tmp.name)
            with open(tmp.name, "rb") as f:
                text = extract_text(f).replace("\n", " ").replace(",", "")
        text = re.sub(r"\s+", " ", text)
        return text

    def _parse_metrics(self, text: str) -> dict:
        """Parses metrics from data."""
        # Extract data
        match_count = re.search(self.regex["metrics"], text)
        if not match_count:
            raise ValueError("Unable to extract data from text, please update the regex.")
        tests = clean_count(match_count.group(1))
        # Create dataframe
        df = {
            "Cumulative total": [tests],
        }
        return df

    def _parse_date(self, text: str, df: dict) -> dict:
        """Gets date from text."""
        text = re.sub(r"(\d+)[a-zA-Z]+", r"\1", text.lower())
        date = extract_clean_date(text, self.regex["date"], "%d %B %Y")
        df["Date"] = [date]
        return pd.DataFrame(df)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data."""
        return df.pipe(self.pipe_metadata)

    def export(self):
        """Exports data to CSV."""
        df = self.read().pipe(self.pipeline)
        # Export to CSV
        self.export_datafile(df, attach=True)


def main():
    Tanzania().export()
