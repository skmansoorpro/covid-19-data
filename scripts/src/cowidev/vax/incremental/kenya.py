import os
import tempfile
import re
import urllib3

import pandas as pd
import pdftotext

from cowidev.utils import clean_count, get_soup
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils.web.download import download_file_from_url
from cowidev.utils import paths

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Kenya:
    def __init__(self):
        self.location = "Kenya"
        self.source_url = "https://www.health.go.ke"
        self.output_file = os.path.join(f"{paths.SCRIPTS.OUTPUT_VAX_MAIN}", f"{self.location}.csv")
        self.last_update = self.get_last_update()

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        links = self._get_list_pdf_urls()
        records = []
        for link in links:
            # print(link)
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
                text = pdftotext.PDF(f)[0]
            text = text.replace("\n", "")
            text = " ".join(text.split()).lower()
        return text

    def _parse_date(self, pdf_text: str) -> str:
        """Parse date from pdf text"""
        regex = r"date: [a-z]+ ([0-9]+.{0,2},? [a-z]+,? 202\d)"
        date_str = re.search(regex, pdf_text).group(1)
        date = str(pd.to_datetime(date_str).date())
        return date

    def _parse_metrics(self, pdf_text: str) -> tuple:
        """Parse metrics from pdf text"""
        total_vaccinations = clean_count(re.search(r"total doses administered ([\d,]+)", pdf_text).group(1))

        people_vaccinated_adults = clean_count(
            re.search(r"partially vaccinated adult population ([\d,]+)", pdf_text).group(1)
        )
        people_vaccinated_teens = clean_count(
            re.search(r"partially vaccinated teenage population\( 15-17yrs\) ([\d,]+)", pdf_text).group(1)
        )
        people_vaccinated = people_vaccinated_adults + people_vaccinated_teens

        people_fully_vaccinated_adults = clean_count(
            re.search(r"fully vaccinated adult population ([\d,]+)", pdf_text).group(1)
        )
        people_fully_vaccinated_teens = clean_count(
            re.search(r"fully vaccinated teenage population\( 15-17yrs\) ([\d,]+)", pdf_text).group(1)
        )
        people_fully_vaccinated = people_fully_vaccinated_adults + people_fully_vaccinated_teens

        total_boosters = clean_count(re.search(r"booster doses ([\d,]+)", pdf_text).group(1))

        return total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for metadata"""
        return df.assign(location=self.location, vaccine="Oxford/AstraZeneca, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for data"""
        return ds.pipe(self.pipe_metadata)

    def get_last_update(self) -> str:
        """Get last update date"""
        return pd.read_csv(self.output_file).date.max()

    def export(self):
        """Export data to csv"""
        df = self.read().pipe(self.pipeline)
        df = merge_with_current_data(df, self.output_file)
        df.to_csv(self.output_file, index=False)


def main():
    Kenya().export()


if __name__ == "__main__":

    main()
