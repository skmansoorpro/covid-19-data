import tempfile
import re

import pandas as pd
import pdftotext

from cowidev.utils import clean_count, clean_date, get_soup
from cowidev.utils.web.download import download_file_from_url
from cowidev.vax.utils.incremental import enrich_data, increment


class Mexico:
    def __init__(self):
        self.source_page = "https://www.gob.mx/salud/documentos/presentaciones-2022"
        self.location = "Mexico"

    def read(self):
        """Read the data from the source"""
        soup = get_soup(self.source_page)
        link = self._parse_link_pdf(soup)
        return self._parse_data(link)

    def _parse_link_pdf(self, soup) -> list:
        """Parse the link to the pdf"""
        link = soup.find(class_="list-unstyled").find("a")["href"]
        link = "http://www.gob.mx" + link
        self.source_url = link
        # print(link)
        return link

    def _get_text_from_pdf(self, url: str) -> str:
        """Get the text from the pdf url"""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url, tmp.name)
            with open(tmp.name, "rb") as f:
                pdf = pdftotext.PDF(f)

        text = "\n\n".join(pdf)
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"(\d)\,(\d)", r"\1\2", text)
        return text

    def _parse_data(self, url: str) -> pd.Series:
        """Parse the data from the pdf url"""
        text = self._get_text_from_pdf(url)
        total_vaccinations = clean_count(
            re.search(r"(\d+) \d+ \d+ Total de dosis aplicadas reportadas", text).group(1)
        )
        date = clean_date(re.search(r"(\d{1,2} \w+\, 20\d{2})", text).group(1), "%d %B, %Y", lang="es")

        matches = re.search(r"Esquema Nuevos (\d+) completo esquemas Personas vacunadas reportadas (\d+)", text)
        people_vaccinated = clean_count(matches.group(1))
        people_fully_vaccinated = clean_count(matches.group(2))

        # Tests
        assert total_vaccinations >= 94300526
        assert people_vaccinated >= 61616895
        assert people_fully_vaccinated >= 41115211
        assert people_vaccinated <= total_vaccinations
        assert people_fully_vaccinated >= 0.5 * people_vaccinated

        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": date,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        """Pipe location to the data"""
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        """Pipe vaccine to the data"""
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Oxford/AstraZeneca, Moderna, Pfizer/BioNTech, Sinovac, Sputnik V, CanSino",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        """Pipe source url to the data"""
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for the data"""
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        """Export the data to a csv"""
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Mexico().export()


if __name__ == "__main__":
    main()
