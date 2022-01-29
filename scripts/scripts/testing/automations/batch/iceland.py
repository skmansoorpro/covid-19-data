import re
import json

import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_date_series, clean_count


class Iceland(CountryTestBase):
    location: str = "Iceland"
    units: str = "tests performed"
    source_label: str = "Government of Iceland"
    source_url_ref: str = "https://www.covid.is/data"
    source_url: str = "https://e.infogram.com/"
    regex: dict = {
        "title_test": r"Fjöldi sýna eftir dögum",
        "title_positive": r"Fjöldi smita innanlands",
        "element": r"window\.infographicData=({.*})",
    }
    rename_columns: str = {
        "Symptomatic tests": "t1",
        "Quarantine- and random tests": "t2",
        "deCODE Genetics screening": "t3",
        "Border tests 1 and 2": "t4",
        "Symptomatic screening": "p1",
        "Quarantine- and random screening": "p2",
        "Screening by deCODE Genetics": "p3",
    }

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        data_id = self._get_data_id_from_source(self.source_url_ref)
        df = self._load_data(data_id)
        return df

    def _get_data_id_from_source(self, source_url: str) -> str:
        """Get Data ID from source"""
        soup = get_soup(source_url)
        data_id = soup.find(class_="infogram-embed")["data-id"]
        return data_id

    def _load_data(self, data_id: str) -> pd.DataFrame:
        """Load data from source"""
        url = f"{self.source_url}{data_id}"
        soup = get_soup(url)
        match = re.search(self.regex["element"], str(soup))
        if not match:
            raise ValueError("Website Structure Changed, please update the script")
        data = json.loads(match.group(1))
        data = data["elements"]["content"]["content"]["entities"]
        data_test = [data[idx] for idx in data if re.search(self.regex["title_test"], str(data[idx].values()))][0]
        data_positive = [
            data[idx] for idx in data if re.search(self.regex["title_positive"], str(data[idx].values()))
        ][0]
        data_list = data_test["props"]["chartData"]["data"]
        data_list.extend(data_positive["props"]["chartData"]["data"])
        df = pd.DataFrame()
        for frame in data_list:
            col = frame.pop(0)
            col[0] = "Date"
            df = df.append(pd.DataFrame(frame, columns=col), ignore_index=True)
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean date"""
        return df.assign(Date=clean_date_series(df["Date"], "%d.%m.%y"))

    def pipe_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric columns"""
        df["t1"] = df["t1"].apply(clean_count)
        df["t2"] = df["t2"].apply(clean_count)
        df["t3"] = df["t3"].apply(clean_count)
        df["p1"] = df["p1"].apply(clean_count)
        df["p2"] = df["p2"].apply(clean_count)
        df["p3"] = df["p3"].apply(clean_count)
        return df

    def pipe_row_sum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sum rows"""
        df = df.assign(positive=df[["p1", "p2", "p3"]].sum(axis=1))
        return df.assign(daily_change=df[["t1", "t2", "t3"]].sum(axis=1))

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process metrics"""
        df = df.groupby("Date", as_index=False).sum()
        df = df.sort_values("Date")
        df = df.assign(**{"Cumulative total": df.daily_change.cumsum()})
        return df

    def pipe_pr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate PR"""
        positives_over_period = df["positive"].diff().abs()
        tests_over_period = df["Cumulative total"].diff()
        return df.assign(**{"Positive rate": (positives_over_period / tests_over_period).round(5)})

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline"""
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_numeric)
            .pipe(self.pipe_row_sum)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_pr)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        """Export data to csv"""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, float_format="%.5f")


def main():
    Iceland().export()


if __name__ == "__main__":
    main()
