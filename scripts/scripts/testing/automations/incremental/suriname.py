import json
import re
from datetime import datetime, timedelta

import pandas as pd
from cowidev.testing import CountryTestBase
from cowidev.utils.clean import clean_date, clean_count
from cowidev.utils.web import get_soup


class Suriname(CountryTestBase):
    location: str = "Suriname"
    units: str = "tests performed"
    source_label: str = "Directorate National Security"
    source_url: str = "https://covid-19.sr/"

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        soup = get_soup(self.source_url)
        date = clean_date(datetime.now())
        count = clean_count(soup.select(".vc_custom_1643848135697 .stats-number")[0]["data-counter-value"])
        negative = clean_count(soup.select(".vc_custom_1643848161956 .stats-number")[0]["data-counter-value"])
        positive = count - negative
        df = pd.DataFrame({"Date": [date], "Daily change in cumulative total": [count], "positive": [positive]})
        return df

    def pipe_pr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Positive Rate"""
        df["Positive rate"] = (
            df["positive"].rolling(7).sum().div(df["Daily change in cumulative total"].rolling(7).sum()).round(3)
        ).fillna(0)
        return df

    def pipe_merge(self, df: pd.DataFrame) -> pd.DataFrame:
        df_current = pd.read_csv(self.get_output_path())
        df_current = df_current[df_current.Date < df.Date.min()]
        df = pd.concat([df_current, df]).sort_values("Date")
        return df

    def pipe_positive(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data processing"""
        return df.pipe(self.pipe_metadata).pipe(self.pipe_merge).pipe(self.pipe_positive).pipe(self.pipe_pr)

    def export(self):
        """Export data to csv"""
        df = self.read().pipe(self.pipeline)
        # self.export_datafile(df, float_format="%.5f")
        df.to_csv(self.get_output_path(), index=False)


def main():
    Suriname().export()


if __name__ == "__main__":
    main()
