import pandas as pd
from tableauscraper import TableauScraper as TS

from cowidev.utils import paths, clean_date_series
from cowidev.vax.utils.incremental import merge_with_current_data


class Thailand:
    location = "Thailand"
    source_url = "https://public.tableau.com/views/SATCOVIDDashboard/1-dash-tiles-w"
    source_url_ref = "https://ddc.moph.go.th/covid19-dashboard/"

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        df = self._parse_metrics(self.source_url)
        return df

    def _parse_metrics(self, url: str) -> pd.DataFrame:
        """Parse metrics from source"""
        ts = TS()
        df = pd.DataFrame()
        ts.loads(url)
        ws = ts.getWorksheet("D_Vac_Stack")
        df["date"] = clean_date_series(ws.data["DAY(txn_date)-value"].drop_duplicates())
        df["people_vaccinated"] = ws.data.loc[
            ws.data["vaccine_plan_group-alias"] == "1", "SUM(vaccine_total_acm)-value"
        ].reset_index(drop=True)
        df["people_fully_vaccinated"] = ws.data.loc[
            ws.data["vaccine_plan_group-alias"] == "2", "SUM(vaccine_total_acm)-value"
        ].reset_index(drop=True)
        df["total_boosters"] = ws.data.loc[
            ws.data["vaccine_plan_group-alias"] == "3", "SUM(vaccine_total_acm)-value"
        ].reset_index(drop=True)
        return df

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Total Vaccinations"""
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters)

    def pipe_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add vaccine information"""
        return df.assign(vaccine="Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac, Moderna")

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata"""
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data"""
        return df.pipe(self.pipe_metrics).pipe(self.pipe_vaccines).pipe(self.pipe_metadata)

    def export(self):
        """Export data to CSV"""
        df = self.read()
        merge_with_current_data(df.pipe(self.pipeline), paths.out_vax(self.location)).to_csv(
            paths.out_vax(self.location), index=False
        )


def main():
    Thailand().export()


if __name__ == "__main__":
    main()
