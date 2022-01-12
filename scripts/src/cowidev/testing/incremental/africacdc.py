import pandas as pd

from cowidev.vax.incremental.africacdc import AfricaCDC as AfricaCDCVax
from cowidev.testing.utils.incremental import increment


ACDC_COUNTRIES = {
    "Angola": {"name": "Angola", "notes": ""},
    "Botswana": {
        "name": "Botswana",
    },
    "Burundi": {
        "name": "Burundi",
    },
    "Burkina Faso": {
        "name": "Burkina Faso",
    },
    "Central African Republic": {
        "name": "Central African Republic",
    },
    "Chad": {
        "name": "Chad",
    },
}
country_mapping = {country: metadata["name"] for country, metadata in ACDC_COUNTRIES.items()}


class AfricaCDC(AfricaCDCVax):
    _base_url = (
        "https://services8.arcgis.com/vWozsma9VzGndzx7/ArcGIS/rest/services/"
        "DailyCOVIDDashboard_5July21_1/FeatureServer/0/"
    )
    source_label = "Africa Centres for Disease Control and Prevention"
    columns_use = [
        "Country",
        "Tests_Conducted",
        "Date",
    ]
    columns_rename = {
        "Country": "location",
        "Tests_Conducted": "Cumulative total",
        "Date": "date",
    }
    units = "tests performed"

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            source_label=self.source_label,
            source_url=self.source_url_ref,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_columns)
            .pipe(self.pipe_rename)
            .pipe(self.pipe_filter_countries, country_mapping)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
        )

    def increment_countries(self, df: pd.DataFrame):
        for row in df.sort_values("location").iterrows():
            row = row[1]
            country = row["location"]
            notes = ACDC_COUNTRIES[country].get("notes", "")
            increment(
                count=row["Cumulative total"],
                sheet_name=country,
                country=country,
                units=self.units,
                date=row["date"],
                source_url=self.source_url,
                source_label=self.source_label,
                notes=notes,
            )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df)


def main():
    AfricaCDC().export()
