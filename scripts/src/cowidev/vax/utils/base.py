import pandas as pd

from cowidev.utils import paths
from cowidev.utils.s3 import S3, obj_from_s3
from cowidev.utils.clean.dates import localdate
from cowidev.utils.clean.numbers import metrics_to_num_int, metrics_to_num_float
from cowidev.vax.utils.files import export_metadata


COLUMNS_ORDER = [
    "location",
    "date",
    "vaccine",
    "source_url",
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
]

COLUMNS_ORDER_AGE = [
    "location",
    "date",
    "age_group_min",
    "age_group_max",
    "people_vaccinated_per_hundred",
    "people_fully_vaccinated_per_hundred",
    "people_with_booster_per_hundred",
]

METRICS = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "people_vaccinated_per_hundred",
    "people_fully_vaccinated_per_hundred",
    "people_with_booster_per_hundred",
]


class CountryVaxBase:
    location: str = None

    def __init__(self):
        if self.location == None:
            raise NotImplementedError("Please define class attribute `location`")

    def from_ice(self):
        """Loads single CSV `location.csv` from S3 as DataFrame."""
        path = f"{paths.S3.VAX_ICE}/{self.location}.csv"
        _check_last_update(path, self.location)
        df = obj_from_s3(path)
        return df

    @property
    def output_path(self):
        """Country output file."""
        return paths.out_vax(self.location)

    @property
    def output_path_age(self):
        """Country output file for age-group data."""
        return paths.out_vax(self.location, age=True)

    @property
    def output_path_manufacturer(self):
        """Country output file for manufacturer data."""
        return paths.out_vax(self.location, manufacturer=True)

    def _postprocessing(self, df):
        """Minor post processing after all transformations.

        Basically sort by date, ensure correct column order, correct type for metrics.
        """
        df = metrics_to_num_int(df, METRICS)
        df = df.sort_values("date")
        cols = [col for col in COLUMNS_ORDER if col in df.columns] + [
            col for col in df.columns if col not in COLUMNS_ORDER
        ]
        df = df[cols]
        return df

    def _postprocessing_age(self, df):
        """Minor post processing after all transformations.

        Basically sort by date, ensure correct column order, correct type for metrics.
        """
        df = metrics_to_num_float(df, METRICS)
        df = df.sort_values(["date", "age_group_min", "age_group_max"])
        cols = [col for col in COLUMNS_ORDER_AGE if col in df.columns] + [
            col for col in df.columns if col not in COLUMNS_ORDER_AGE
        ]
        df = df[cols]
        return df

    def export_datafile(
        self,
        df,
        df_age=None,
        df_manufacturer=None,
        meta_age=None,
        meta_manufacturer=None,
        attach=False,
        reset_index=False,
        **kwargs,
    ):
        """Export country data.

        Args:
            df (pd.DataFrame): Main country data.
            df_age (pd.DataFrame, optional): Country data by age group. Defaults to None.
            df_manufacturer (pd.DataFrame, optional): Country data by manufacturer. Defaults to None.
            meta_age (dict, optional): Country metadata by age. Defaults to None.
            meta_manufacturer (dict, optional): Country metadata by manufacturer. Defaults to None.
            attach (bool, optional): Set to True to attach to already existing data. Defaults to False.
            reset_index (bool, optional): Brin index back as a column. Defaults to False.
        """
        self._export_datafile_main(df, attach=attach, reset_index=reset_index, **kwargs)
        if df_age is not None:
            self._export_datafile_age(df_age, meta_age)
        if df_manufacturer is not None:
            self._export_datafile_manufacturer(df_manufacturer, meta_manufacturer)

    def _export_datafile_main(self, df, attach=False, reset_index=False, **kwargs):
        """Export main data."""
        if attach:
            df = merge_with_current_data(df, self.output_path)
        df = self._postprocessing(df)
        if reset_index:
            df = df.reset_index(drop=True)
        df.to_csv(self.output_path, index=False, **kwargs)

    def _export_datafile_age(self, df, metadata):
        """Export age data."""
        df = self._postprocessing_age(df)
        self._export_datafile_secondary(df, metadata, self.output_path_age, paths.SCRIPTS.OUTPUT_VAX_META_AGE)

    def _export_datafile_manufacturer(self, df, metadata):
        """Export manufacturer data"""
        self._export_datafile_secondary(
            df, metadata, self.output_path_manufacturer, paths.SCRIPTS.OUTPUT_VAX_META_MANUFACT
        )

    def _export_datafile_secondary(self, df, metadata, output_path, output_path_meta):
        """Export secondary data."""
        # Check metadata
        self._check_metadata(metadata)
        # Export data
        df.to_csv(output_path, index=False)
        # Export metadata
        export_metadata(df, metadata["source_name"], metadata["source_url"], output_path_meta)

    def _check_metadata(self, metadata):
        if not isinstance(metadata, dict):
            raise ValueError("The `metadata` object must be a dictionary!")
        if ("source_name" not in metadata) or ("source_url" not in metadata):
            raise ValueError("`metadata` must contain keys 'source_name' and 'source_url'")
        if not (isinstance(metadata["source_name"], str) and isinstance(metadata["source_url"], str)):
            raise ValueError("metadata['source_name'] and metadata['source_url'] must be strings!")

    def _check_attributes(self, mapping):
        for field_raw, field in mapping.items():
            if field is None:
                raise ValueError(f"Please check class attribute {field_raw}, it can't be None!")

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            "location": self.location,
            "source_url": self.source_url_ref,
        }
        mapping = {k: v for k, v in mapping.items() if k not in df}
        self._check_attributes(mapping)
        return df.assign(**mapping)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)


def _check_last_update(path, country):
    metadata = S3().get_metadata(path)
    last_update = metadata["LastModified"]
    now = localdate(force_today=True, as_datetime=True)
    num_days = (now - last_update).days
    if num_days > 4:  # Allow maximum 4 days delay
        raise FileExistsError(
            f"ICE File for {country} is too old ({num_days} days old)! Please check cowidev.vax.icer"
        )


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    df_current = pd.read_csv(filepath)
    df_current = df_current[df_current.Date < df.Date.min()]
    df = pd.concat([df_current, df]).sort_values("Date")
    return df
