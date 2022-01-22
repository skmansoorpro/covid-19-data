import io
import requests

import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.testing.utils import make_monotonic
from cowidev.utils import clean_date_series


class Australia(CountryTestBase):
    location: str = "Australia"
    units: str = "tests performed"
    notes: str = "Made available by CovidbaseAU"
    source_url: str = "https://covidbaseau.com/historical/COVID-19%20Tests%20Australia.csv"
    source_url_ref: str = "https://covidbaseau.com/tests/"
    source_label: str = "Australian Government Department of Health"
    rename_columns: str = {
        "Total": "Cumulative total",
        "Total_Increase": "Daily change in cumulative total",
    }

    def read(self) -> pd.DataFrame:
        response = requests.get(self.source_url)
        df = pd.read_csv(
            io.StringIO(response.content.decode()),
            header=1,
            usecols=["Date", "Total", "Total Increase"],
        )
        df.columns = df.columns.str.replace(" ", "_")
        df = df.drop(list(range(42)))
        df = self._clean_metrics(df)

        return df

    def _clean_metrics(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Change the metrics from str to int"""
        df.replace({"-": 0, ",": ""}, regex=True, inplace=True)
        df = df.assign(
            Total_Increase=pd.to_numeric(df.Total_Increase), Total=pd.to_numeric(df.Total)
        )
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(Date=clean_date_series(df["Date"], "%d %b %y"))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Australia().export()


if __name__ == "__main__":
    main()
