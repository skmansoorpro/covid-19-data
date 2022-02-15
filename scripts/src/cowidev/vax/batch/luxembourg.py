import pandas as pd

from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.utils import add_latest_who_values


def read(source: str) -> pd.DataFrame:
    df = pd.read_excel(source)
    check_known_columns(
        df,
        [
            "Date",
            "Nombre de dose 1",
            "Nombre de dose 2",
            "Nombre de Dose complémentaire par rapport à schéma complet",
            "Nombre total de doses",
        ],
    )
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "Date": "date",
            "Nombre de dose 1": "people_vaccinated",
            "Nombre de dose 2": "people_fully_vaccinated",
            "Nombre de Dose complémentaire par rapport à schéma complet": "total_boosters",
            "Nombre total de doses": "total_vaccinations",
        }
    )


def correct_time_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Since 2021-04-14 Luxembourg is using J&J, therefore dose2 == people_fully_vaccinated no longer
    works. As a temporary fix while they report the necessary data, we're inserting the latest value
    reported to the WHO.
    The publisher was contacted on 2021-O9-21 https://twitter.com/redouad/status/1439992459166158857
    """
    df.loc[df.date >= "2021-04-14", "people_fully_vaccinated"] = pd.NA
    df = add_latest_who_values(df, "Luxembourg", ["people_fully_vaccinated"])
    return df


def calculate_running_totals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date")
    df[["people_vaccinated", "people_fully_vaccinated", "total_boosters", "total_vaccinations"]] = df[
        ["people_vaccinated", "people_fully_vaccinated", "total_boosters", "total_vaccinations"]
    ].cumsum()
    return df


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Luxembourg")


def enrich_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(vaccine="Pfizer/BioNTech")
    df.loc[df.date >= "2021-01-20", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df.date >= "2021-02-10", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[df.date >= "2021-04-14", "vaccine"] = "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df


def enrich_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.assign(source_url=source)


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df.pipe(rename_columns)
        .pipe(calculate_running_totals)
        .pipe(enrich_source, source)
        .pipe(correct_time_series)
        .pipe(enrich_location)
        .pipe(enrich_vaccines)
    )


def main():
    source_file = "https://data.public.lu/en/datasets/r/2635e6af-bd22-4e62-8525-48fd3cb063e6"
    source_page = "https://data.public.lu/en/datasets/donnees-covid19/#_"
    destination = paths.out_vax("Luxembourg")
    read(source_file).pipe(pipeline, source_page).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
