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
        "date": "Date",
        "average_daily_number_of_art_swabs_tested_over_the_past_week": "art",
        "average_daily_number_of_pcr_swabs_tested": "pcr",
    }

    def _read_art(self):
        url = f"{self.base_url}1ee4d904-b17e-41de-a731-854578b036e6"
        json_dict = request_json(url)["result"]["records"]
        df = pd.DataFrame.from_records(json_dict).drop(columns=["_id"])
        df["date"][(df[df["date"] == "2021-12-12"].index.values)] = "2021-12-21"
        df["average_daily_number_of_art_swabs_tested_over_the_past_week"] = df[
            "average_daily_number_of_art_swabs_tested_over_the_past_week"
        ].apply(clean_count)
        return df

    def _read_pcr(self):
        url = f"{self.base_url}07cd6bfd-c73e-4aed-bc7b-55b13dd9e7c2"
        json_dict = request_json(url)["result"]["records"]
        df = pd.DataFrame.from_records(json_dict).drop(columns=["_id"])
        df["average_daily_number_of_pcr_swabs_tested"] = df["average_daily_number_of_pcr_swabs_tested"].apply(
            clean_count
        )
        return df

    def read(self):
        art = self._read_art()
        pcr = self._read_pcr()
        df = pd.merge(art, pcr, how="outer").sort_values(by="date")
        df["Daily change in cumulative total"] = (
            df["average_daily_number_of_art_swabs_tested_over_the_past_week"]
            + df["average_daily_number_of_pcr_swabs_tested"]
        )
        df["date"] = pd.to_datetime(df["date"])

        i = 0
        z = len(df)
        while i < (z * 7):
            new = pd.DataFrame(
                {
                    "date": pd.date_range(
                        df["date"][i] + timedelta(days=1), df["date"][i] + timedelta(days=6)
                    ).to_list(),
                    "average_daily_number_of_art_swabs_tested_over_the_past_week": df[
                        "average_daily_number_of_art_swabs_tested_over_the_past_week"
                    ][i],
                    "average_daily_number_of_pcr_swabs_tested": df["average_daily_number_of_pcr_swabs_tested"][i],
                    "Daily change in cumulative total": df["Daily change in cumulative total"][i],
                }
            )
            df = df.append(new).sort_values(by="date").reset_index(drop=True)
            i += 7
        print(df.dropna().tail(15))
        return df.dropna()

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        df["Cumulative total"] = pd.NA
        self.export_datafile(df)


def main():
    Singapore().export()


if __name__ == "__main__":
    main()
