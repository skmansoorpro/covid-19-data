from multiprocessing.sharedctypes import Value
import pandas as pd
from datetime import timedelta


from cowidev.testing import CountryTestBase
from cowidev.utils import clean_date_series, clean_count
from cowidev.utils.web import request_json


class Singapore(CountryTestBase):
    location = "Singapore"
    units = "samples tested"
    base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
    source_url_ref = "https://www.moh.gov.sg/covid-19/statistics"
    source_label = "Ministry of Health Singapore"
    rename_columns = {
        "week_of": "Date",
        "average_daily_number_of_art_swabs_tested_over_the_past_week": "art",
        "average_daily_number_of_pcr_swabs_tested": "pcr",
    }

    def _read_art(self):
        url = f"{self.base_url}1ee4d904-b17e-41de-a731-854578b036e6"
        json_dict = request_json(url)["result"]["records"]
        df = pd.DataFrame.from_records(json_dict).drop(columns=["_id"])
        # correct errors
        df.loc[(df[df["week_of"] == "14/12/2022"].index.values), "week_of"] = "14/12/2021"
        df.loc[(df[df["week_of"] == "28/12/2022"].index.values), "week_of"] = "28/12/2021"
        df["week_of"] = clean_date_series(df["week_of"], "%d/%m/%Y")
        return df

    def _read_pcr(self):
        url = f"{self.base_url}07cd6bfd-c73e-4aed-bc7b-55b13dd9e7c2"
        json_dict = request_json(url)["result"]["records"]
        df = pd.DataFrame.from_records(json_dict).drop(columns=["_id"])
        df = df.rename(columns={"date": "week_of"})
        return df

    def read(self):
        # Read both source data and merge
        art = self._read_art()
        pcr = self._read_pcr()
        df = pd.merge(art, pcr).sort_values(by="week_of")
        return df

    def pipe_fill_gaps(self, df: pd.DataFrame):
        """Fill gaps with average daily value."""
        df["Date"] = pd.to_datetime(df["Date"])
        # Check difference of 7 days
        if not (df.Date.diff().iloc[1:].dt.days == 7).all():
            raise ValueError("Not all values are separated by 7 days. Please check `Date` value!")
        # Create date range
        dt_min = df.Date.min()
        dt_max = df.Date.max() + timedelta(days=6)
        ds = pd.Series(pd.date_range(dt_min, dt_max), name="Date")
        # Merge with dataframe & fillna
        df = df.merge(ds, how="outer", sort=True)  # .sort_values("date")
        df = df.fillna(method="ffill")
        df["Date"] = clean_date_series(df["Date"])
        return df

    def pipe_metric(self, df: pd.DataFrame):
        return df.assign(
            **{
                "Daily change in cumulative total": df.art.apply(clean_count) + df.pcr.apply(clean_count),
                "Cumulative total": pd.NA,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns).pipe(self.pipe_fill_gaps).pipe(self.pipe_metric).pipe(self.pipe_metadata)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Singapore().export()
