import re
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup

from cowidev.utils.clean import clean_date_series, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.utils.web.download import read_xlsx_from_url
from cowidev.utils import paths
from cowidev.vax.utils.utils import build_vaccine_timeline
from cowidev.vax.utils.base import CountryVaxBase


class NewZealand(CountryVaxBase):
    # Consider: https://github.com/minhealthnz/nz-covid-data/tree/main/vaccine-data
    source_url = "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-vaccine-data"
    location = "New Zealand"
    columns_rename = {
        "First doses": "people_vaccinated_12",
        "Second doses": "people_fully_vaccinated_12",
        "Third primary doses": "third_dose_12",
        "Boosters": "total_boosters_12",
        "Date": "date",
    }
    columns_by_age_group_rename = {
        "# doses administered": "total_vaccinations",
        "Ten year age group": "age_group",
    }
    columns_cumsum = ["people_vaccinated_12", "people_fully_vaccinated_12", "third_dose_12", "total_boosters_12"]
    columns_cumsum_by_age = ["Ten year age group"]

    def read(self) -> pd.DataFrame:
        soup = get_soup(self.source_url)
        self._read_latest(soup)
        link = self._parse_file_link(soup)
        df = read_xlsx_from_url(link, sheet_name="Date")
        return df

    def _read_latest(self, soup):
        tables = pd.read_html(str(soup))
        latest = tables[0].set_index("Unnamed: 0")
        latest_kids = tables[1].set_index("Unnamed: 0")
        latest_date = re.search(r"Data in this section is as at 11:59pm ([\d]+ [A-Za-z]+ 202\d)", soup.text).group(1)
        self.latest = pd.DataFrame(
            {
                "total_vaccinations_12": latest.loc["Total doses", "Cumulative total"],
                # "total_vaccinations_5_12": latest_kids.loc["First dose", "Cumulative total"],
                "people_vaccinated_12": latest.loc["First dose", "Cumulative total"],
                "people_vaccinated_5_12": latest_kids.loc["First dose", "Cumulative total"],
                "people_fully_vaccinated_12": latest.loc["Second dose", "Cumulative total"],
                "total_boosters_12": latest.loc["Boosters", "Cumulative total"],
                "third_dose_12": latest.loc["Third primary", "Cumulative total"],
                "date": [clean_date(latest_date, fmt="%d %B %Y", lang="en")],
            }
        )

    def _parse_file_link(self, soup: BeautifulSoup) -> str:
        href = soup.find(id="download").find_next("a")["href"]
        link = f"https://{urlparse(self.source_url).netloc}/{href}"
        return link

    def pipe_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns_rename:
            return df.rename(columns=self.columns_rename)
        return df

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns_cumsum:
            df[self.columns_cumsum] = df[self.columns_cumsum].cumsum()
        return df

    def pipe_merge_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df_current = self.load_datafile(usecols=["date", "people_vaccinated_5_12"])
        df["date"] = df.date.astype(str)
        df = df.merge(df_current, on="date")
        return pd.concat([df, self.latest]).drop_duplicates("date", keep="last").reset_index(drop=True)

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        people_vaccinated = df.people_vaccinated_12 + df.people_vaccinated_5_12.fillna(0)
        people_fully_vaccinated = df.people_fully_vaccinated_12  # + df.people_fully_vaccinated_5_12
        total_boosters = df.total_boosters_12 + df.third_dose_12  # + df.total_boosters_5_12 + df.third_dose_5_12
        return df.assign(
            total_vaccinations=people_vaccinated + people_fully_vaccinated + total_boosters,
            people_vaccinated=people_vaccinated,
            people_fully_vaccinated=people_fully_vaccinated,
            total_boosters=total_boosters,
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date))

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return build_vaccine_timeline(df, {"Pfizer/BioNTech": "2021-01-01", "Oxford/AstraZeneca": "2021-11-26"})

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url,
        )

    def pipe_columns_out(self, df: pd.DataFrame):
        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
                "total_vaccinations",
                # "people_vaccinated_12",
                "people_vaccinated_5_12",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename)
            .pipe(self.pipe_cumsum)
            .pipe(self.pipe_merge_data)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_date)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_columns_out)
        )

    def pipe_rename_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.columns_by_age_group_rename:
            return df.rename(columns=self.columns_by_age_group_rename)
        return df

    def pipe_aggregate_by_age(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.groupby("Ten year age group")["# doses administered"].sum().reset_index()
        return df

    def pipe_postprocess(self, df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split(r" to |\+\/Unknown", expand=True)
        df["date"] = date_str
        df = df[["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]]
        return df

    def pipeline_by_age(self, df: pd.DataFrame, date_str: str) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_aggregate_by_age)
            .pipe(self.pipe_rename_by_age)
            .pipe(self.pipe_location)
            .pipe(self.pipe_postprocess, date_str)
        )

    def export(self):
        self.read().pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)


def main():
    NewZealand().export()


if __name__ == "__main__":
    main()
