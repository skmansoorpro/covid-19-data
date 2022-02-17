import os
import tempfile
import re

import pandas as pd
from pdfminer.high_level import extract_text

from cowidev.utils import clean_count, get_soup
from cowidev.utils.clean import extract_clean_date
from cowidev.utils.web.download import download_file_from_url
from cowidev.vax.utils.base import CountryVaxBase


class Kenya(CountryVaxBase):
    location = "Kenya"
    source_url = "https://www.health.go.ke"
    regex = {
        "date": r"date: [a-z]+ ([0-9]+).{0,2},? ([a-z]+),? (202\d)",
        "metrics": {
            "total_vaccinations": r"total doses administered ([\d,]+)",
            "people_vaccinated_adults": r"proportion of adults fully vaccinated ([\d,]+)",
            "people_vaccinated_teens": r"partially vaccinated teenage population\( 15-below 18yrs\) ([\d,]+)",
            "people_fully_vaccinated_adults": r"proportion of adults fully vaccinated [\d,]+ ([\d,]+)",
            "people_fully_vaccinated_teens": r"fully vaccinated teenage population\( 15-below 18yrs\) ([\d,]+)",
            "total_boosters": r"booster doses ([\d,]+)",
        },
    }

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        links = self._get_list_pdf_urls()
        records = []
        for link in links:
            print(link)
            text = self._get_text_from_pdf(link)
            date = self._parse_date(text)
            if date <= self.last_update:
                break
            total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters = self._parse_metrics(text)
            records.append(
                {
                    "total_vaccinations": total_vaccinations,
                    "people_vaccinated": people_vaccinated,
                    "people_fully_vaccinated": people_fully_vaccinated,
                    "total_boosters": total_boosters,
                    "date": date,
                    "source_url": link,
                }
            )
        assert len(records) > 0, f"No new record found after {self.last_update}"
        return pd.DataFrame(records)

    def _get_list_pdf_urls(self) -> list:
        """Get list of pdf urls"""
        soup = get_soup(self.source_url, verify=False)
        links = list(
            map(lambda x: x.get("href"), soup.findAll("a", text=re.compile("MINISTRY OF HEALTH KENYA COVID-19")))
        )
        return links

    def _get_text_from_pdf(self, url_pdf: str) -> str:
        """Get text from pdf file"""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url_pdf, tmp.name, verify=False)
            with open(tmp.name, "rb") as f:
                text = extract_text(f)
        text = text.replace("\n", " ")
        text = " ".join(text.split()).lower()
        return text

    def _parse_date(self, pdf_text: str) -> str:
        """Parse date from pdf text"""
        return extract_clean_date(pdf_text, self.regex["date"], "%d %B %Y")

    def _parse_metrics(self, pdf_text: str) -> tuple:
        """Parse metrics from pdf text"""
        # Extract metrics from text
        metrics = {
            metric: clean_count(re.search(regex, pdf_text).group(1)) for metric, regex in self.regex["metrics"].items()
        }
        # Process and get new metrics
        metrics = {
            **metrics,
            **{
                "people_vaccinated": metrics["people_vaccinated_adults"] + metrics["people_vaccinated_teens"],
                "people_fully_vaccinated": (
                    metrics["people_fully_vaccinated_adults"] + metrics["people_fully_vaccinated_teens"]
                ),
            },
        }
        return (
            metrics["total_vaccinations"],
            metrics["people_vaccinated"],
            metrics["people_fully_vaccinated"],
            metrics["total_boosters"],
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for metadata"""
        return df.assign(location=self.location, vaccine="Oxford/AstraZeneca, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for data"""
        return ds.pipe(self.pipe_metadata)

    @property
    def last_update(self) -> str:
        """Get last update date"""
        return self.load_datafile().date.max()

    def export(self):
        """Export data to csv"""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, attach=True)


def main():
    Kenya().export()
