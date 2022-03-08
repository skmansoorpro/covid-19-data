import tempfile
import re

import pandas as pd
from pdfminer.high_level import extract_text
import tabula

from cowidev.utils import clean_count, get_soup
from cowidev.utils.clean import extract_clean_date
from cowidev.utils.web.download import download_file_from_url
from cowidev.vax.utils.base import CountryVaxBase


class Kenya(CountryVaxBase):
    location = "Kenya"
    source_url = "https://www.health.go.ke"
    regex = {
        "date": r"date: [a-z]+ ([0-9]+).{0,2},? ([a-z]+),? (202\d)",
    }

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        links = self._get_list_pdf_urls()
        # links = [
        #     l
        #     for l in links
        #     if "23RD-FEBRUARY-2022" not in l
        #     and "18TH-FEBRUARY-2022" not in l
        #     and "15TH-FEBRUARY-2022" not in l
        #     and "14TH-FEBRUARY-2022" not in l
        # ]
        records = []
        # print(links[-1])
        for link in links:
            print(link)
            date = self._parse_date(link)
            if date <= self.last_update:
                break
            dfs = self._get_tables_from_pdf(link)
            df1, df2 = self._build_dfs(dfs)
            data = self._parse_metrics(df1, df2)
            records.append(
                {
                    **data,
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

    def _get_tables_from_pdf(self, url_pdf: str) -> str:
        """Get text from pdf file"""
        # Read all tables
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url_pdf, tmp.name, verify=False)
            dfs = tabula.read_pdf(tmp.name, pages="all")
        return dfs

    def _build_dfs(self, dfs):
        colname = "Current Status"
        df1 = dfs[0].dropna(axis=0, how="all")
        if not (colname in df1.columns) or (df1.shape != (8, 2)):
            raise ValueError("Table 1 has changed, please check!")
        colname = "Age"
        df2 = dfs[2].dropna(axis=0, how="all")
        if not (df2.shape == (8, 4) or df2.shape == (7, 4) or df2.shape == (8, 3) or df2.shape == (7, 3)):
            raise ValueError("Table 3 has changed, please check!")
        return df1, df2

    def _parse_date(self, url: str) -> str:
        """Parse date from pdf text"""
        rex = ".*\-(\d+)\w+\-(\w+)\-(20\d\d).*.pdf"
        return extract_clean_date(url, rex, "%d %B %Y")

    def _parse_metrics(self, df1, df2) -> dict:
        """Parse metrics from pdf text"""
        # First df
        column_value = "Total doses Administered"
        msk = df1["Current Status"].str.contains("Total Doses Administered")
        total_vaccinations = df1.loc[msk, column_value].apply(clean_count).sum()
        msk = df1["Current Status"].str.contains("Partially")
        people_vaccinated = df1.loc[msk, column_value].apply(clean_count).sum()
        msk = df1["Current Status"].str.contains("Fully vaccinated")
        people_fully_vaccinated = df1.loc[msk, column_value].apply(clean_count).sum()
        msk = df1["Current Status"].str.contains("Booster Doses")
        total_boosters = df1.loc[msk, column_value].apply(clean_count).sum()
        # Second df
        msk1 = df2.filter(regex="Age").squeeze().str.contains("Total Above 18 yrs")
        msk2 = df2.filter(regex="Age").squeeze().str.contains("15- Below 18 yrs")
        msk = msk1 | msk2
        single_doses = df2.loc[msk, "Johnson & Johnson"].apply(clean_count).sum()
        second_doses = df2.loc[msk, "Dose 2"].apply(clean_count).sum()

        # print(single_doses, second_doses, people_fully_vaccinated)
        diff = abs(single_doses + second_doses - people_fully_vaccinated)
        if not diff < 20:
            raise ValueError(f"single doses + second doses != people fully vaccinated // difference of {diff}!")
        people_vaccinated += single_doses

        assert people_vaccinated + people_fully_vaccinated + total_boosters - single_doses == total_vaccinations
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
        }

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for metadata"""
        return df.assign(
            location=self.location,
            vaccine="Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing",
        )

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
