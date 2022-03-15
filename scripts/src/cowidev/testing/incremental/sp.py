import requests

from bs4 import BeautifulSoup
import pandas as pd
import tabula

from cowidev.utils import get_soup
from cowidev.utils.log import get_logger
from cowidev.utils.clean import extract_clean_date
from cowidev.testing.utils.orgs import SPRO_COUNTRIES
from cowidev.testing.utils.base import CountryTestBase

logger = get_logger()


class SPRO(CountryTestBase):
    location: str = "SPRO"  # Arbitrary location to pass checks
    units: str = "tests performed"
    source_url: str = "https://app.powerbi.com/view?r=eyJrIjoiMTQwZmJmZjctMjkwMC00MThkLWI5NDgtNmQ3OGUwNDc4ZWE3IiwidCI6IjBmOWUzNWRiLTU0NGYtNGY2MC1iZGNjLTVlYTQxNmU2ZGM3MCIsImMiOjh9"
    source_label: str = "WHO Regional Office for the South Pacific"

    ### Scraping functions go here

    def pipe_rename_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames countries to match OWID naming convention."""
        df["location"] = df.location.replace(SPRO_COUNTRIES)
        return df

    def pipe_filter_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Gets valid entries:
        - Countries not coming from OWID (avoid loop)
        """
        df = df[df.location.isin(SPRO_COUNTRIES.values())]
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds metadata to DataFrame"""
        mapping = {
            "Country": df["location"],
            "Units": self.units,
            "Notes": self.notes,
            "Source URL": self.source_url_ref,
            "Source label": self.source_label,
            "Date": self.date,
        }
        mapping = {k: v for k, v in mapping.items() if k not in df}
        self._check_attributes(mapping)
        return df.assign(**mapping)

    def increment_countries(self, df: pd.DataFrame):
        """Exports data to the relevant csv and logs the confirmation."""
        locations = set(df.location)
        for location in locations:
            df_c = df[df.location == location]
            df_c = df_c.dropna(
                subset=["Cumulative total"],
                how="all",
            )
            if not df_c.empty:
                self.export_datafile(df_c, filename=location, attach=True)
                logger.info(f"\tcowidev.testing.incremental.spro.{location}: SUCCESS ✅")

    def pipeline(self, df: pd.DataFrame):
        """Pipeline for data"""
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_rename_countries)
            .pipe(self.pipe_filter_entries)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        """Exports data to csv."""
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df)


def main():
    SPRO().export()
