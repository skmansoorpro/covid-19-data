import pandas as pd

from cowidev.utils.web import request_json
from cowidev.utils.clean import clean_date_series, clean_count
from cowidev.testing.utils import make_monotonic
from cowidev.testing import CountryTestBase


class Qatar(CountryTestBase):
    location: str = "Qatar"
    units: str = "people tested"
    source_label: str = "Qatar Ministry of Public Health"
    source_url: str = "https://covid19.moph.gov.qa/EN/_api/web/lists/getbytitle('Covid19DailyStatus')/items?$top=5000"
    source_url_ref: str = "https://covid19.moph.gov.qa/EN/Pages/default.aspx"
    rename_columns: dict = {
        "PublishingDate": "Date",
        "NumPositiveCases24H": "positive",
        "TotalPeopleTestedToDate": "Cumulative total",
    }
    header: dict = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "application/json; odata=verbose",
    }
    date_start = "2021-10-28"

    def read(self) -> pd.DataFrame:
        """Reads data from source."""
        data = request_json(self.source_url, headers=self.header)
        df = pd.json_normalize(data, record_path=["d", "results"])
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans date column"""
        return df.assign(Date=clean_date_series(df["Date"], "%Y-%m-%dT%H:%M:%SZ"))

    def pipe_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter data"""
        df = df[(df["Date"] >= self.date_start)]
        return df

    def pipe_metrics(self, df: pd.DataFrame):
        """Pipes metrics"""
        return df.assign(
            **{
                "Cumulative total": df["Cumulative total"].apply(clean_count),
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """pipeline for data"""
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_filter)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic)
            .sort_values("Date")
            .drop_duplicates(subset=["Cumulative total"], keep="first")
        )

    def export(self):
        """Exports data to csv"""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, attach=True)


def main():
    Qatar().export()
