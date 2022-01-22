import os
import tempfile
import re

import requests
import pandas as pd
import PyPDF2

from cowidev.utils.clean import clean_count
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils.web import get_soup
from cowidev.utils import paths


class Kenya:
    def __init__(self):
        self.location = "Kenya"
        self.source_url = "https://www.health.go.ke"
        self.output_file = os.path.join(f"{paths.SCRIPTS.OUTPUT_VAX_MAIN}", f"{self.location}.csv")
        self.last_update = self.get_last_update()

    def read(self):
        links = self._get_list_pdf_urls()
        records = []
        for link in links:
            print(link)
            pages = self._get_text_from_pdf(link)
            date = self._parse_date(pages[0])
            if date <= self.last_update:
                break
            total_vaccinations, people_vaccinated, people_fully_vaccinated, total_boosters = self._parse_metrics(
                pages[0]
            )
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

    def _get_list_pdf_urls(self):
        soup = get_soup(self.source_url, verify=False)
        links = list(
            map(lambda x: x.get("href"), soup.findAll("a", text=re.compile("MINISTRY OF HEALTH KENYA COVID-19")))
        )
        return links

    def _get_text_from_pdf(self, url_pdf: str) -> str:
        def _extract_pdf_text(reader, n):
            page = reader.getPage(n)
            text = page.extractText().replace("\n", "")
            text = " ".join(text.split()).lower()
            return text

        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url_pdf, verify=False).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                pages = [_extract_pdf_text(reader, n) for n in range(reader.numPages)]
        return pages

    def _parse_date(self, pdf_text: str):
        regex = r"date: [a-z]+ ([0-9]+.{0,2},? [a-z]+,? 202\d)"
        date_str = re.search(regex, pdf_text).group(1)
        date = str(pd.to_datetime(date_str).date())
        return date

    def _parse_metrics(self, pdf_text: str):
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
        return df.assign(location=self.location, vaccine="Oxford/AstraZeneca, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_metadata)

    def get_last_update(self):
        return pd.read_csv(self.output_file).date.max()

    def export(self):
        df = self.read().pipe(self.pipeline)
        df = merge_with_current_data(df, self.output_file)
        df.to_csv(self.output_file, index=False)


def main():
    Kenya().export()


if __name__ == "__main__":

    main()
