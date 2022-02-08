import io
import requests

import pandas as pd

from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.vax.utils.utils import build_vaccine_timeline


class HongKong:
    location: str = "Hong Kong"
    source_url: str = " https://www.fhb.gov.hk/download/opendata/COVID19/vaccination-rates-over-time-by-age.csv"
    source_url_ref: str = "https://data.gov.hk/en-data/dataset/hk-fhb-fhbcovid19-vaccination-rates-over-time-by-age"
    vaccine_mapping: dict = {
        "Sinovac": "Sinovac",
        "BioNTech": "Pfizer/BioNTech",
    }

    def read(self) -> pd.DataFrame:
        response = requests.get(self.source_url).content
        df = pd.read_csv(io.StringIO(response.decode("utf-8")))
        check_known_columns(
            df,
            [
                "Date",
                "Age Group",
                "Sex",
                "Sinovac 1st dose",
                "Sinovac 2nd dose",
                "Sinovac 3rd dose",
                "BioNTech 1st dose",
                "BioNTech 2nd dose",
                "BioNTech 3rd dose",
            ],
        )
        return df

    def pipe_reshape(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.rename(columns={"Date": "date"})
            .drop(columns=["Age Group", "Sex"])
            .melt(id_vars=["date"], value_name="total_vaccinations")
            .groupby(["date", "variable"], as_index=False)
            .sum()
        )
        df[["vaccine", "dose"]] = df.variable.str.extract(r"^(\w+) (.*)$")
        df = df.drop(columns="variable").sort_values("date")
        df["total_vaccinations"] = df.groupby(["vaccine", "dose"]).cumsum()
        return df

    def pipe_add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_reshape)

    def pipe_calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.groupby(["date", "dose"], as_index=False)
            .sum()
            .pivot(index="date", columns="dose", values="total_vaccinations")
            .rename(
                columns={
                    "1st dose": "people_vaccinated",
                    "2nd dose": "people_fully_vaccinated",
                    "3rd dose": "total_boosters",
                }
            )
            .reset_index()
        )
        df["total_vaccinations"] = df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters
        return df

    def pipe_add_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return build_vaccine_timeline(
            df,
            {
                "Sinovac": "2021-02-22",
                "Pfizer/BioNTech": "2021-03-06",
            },
        )

    def pipeline_vax(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_calculate_metrics).pipe(self.pipe_add_vaccines).pipe(self.pipe_add_metadata)

    def pipe_sum_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        assert set(df["vaccine"].unique()) == set(self.vaccine_mapping.keys())
        df = df.drop(columns="dose").replace(self.vaccine_mapping).groupby(["date", "vaccine"], as_index=False).sum()
        return df[df.total_vaccinations > 0]

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_sum_manufacturer).assign(location=self.location)

    def export(self):
        df_base = self.read().pipe(self.pipeline_base)

        # Main data
        destination = paths.out_vax(self.location)
        df = df_base.pipe(self.pipeline_vax)
        df.to_csv(destination, index=False)

        # Manufacturer
        destination = paths.out_vax(self.location, manufacturer=True)
        df_manuf = df_base.pipe(self.pipeline_manufacturer)
        df_manuf.to_csv(destination, index=False)
        export_metadata_manufacturer(df_manuf, "Food and Health Bureau", self.source_url_ref)


def main():
    HongKong().export()


if __name__ == "__main__":
    main()
