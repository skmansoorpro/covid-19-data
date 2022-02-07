import datetime

import pandas as pd
import tabula

from cowidev.utils.clean import clean_count
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.incremental import increment


class SriLanka:
    def __init__(self):
        self.location = "Sri Lanka"

    def read(self):
        date = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m_%d")
        pdf_path = self._build_link_pdf(date)
        data = self.parse_data(pdf_path)

        data["date"] = date.replace("_", "-")
        data["location"] = self.location
        data["source_url"] = pdf_path
        data["vaccine"] = "Oxford/AstraZeneca, Sinopharm/Beijing, Sputnik V, Pfizer/BioNTech, Moderna"

        return pd.Series(data=data)

    def _build_link_pdf(self, date):
        return f"http://www.epid.gov.lk/web/images/pdf/corona_vaccination/covid_vaccination_{date}.pdf"

    def parse_data(self, pdf_path):

        dfs = tabula.read_pdf(pdf_path)
        df = dfs[0]

        # All calculations below assume a fixed shape of the PDF's table, and a specific order for
        # the columns and vaccines. If the following tests fail, then the table should be checked
        # for potential changes.
        assert df.shape == (19, 10)
        check_known_columns(
            df,
            [
                "දිය",
                "ේ ාවිෂීල්ඩඩ් Covishield",
                "ටයිේයාෆාම් Sinopharm",
                "ටුට්නිව් - V",
                "Sputnik - V",
                "Unnamed: 0",
                "ෆයිසර්Pfizer",
                "Unnamed: 1",
                "Unnamed: 2",
                "ම ොඩර්ර්ො Moderna",
            ],
        )

        values = df.iloc[16].values
        doses = {
            "first": {
                "covishield": clean_count(values[1].split(" ")[0]),
                "sinopharm": clean_count(values[2].split(" ")[0]),
                "sputnik": clean_count(values[3]),
                "pfizer": clean_count(values[5]),
                "moderna": clean_count(values[9].split(" ")[0]),
            },
            "second": {
                "covishield": clean_count(values[1].split(" ")[1]),
                "sinopharm": clean_count(values[2].split(" ")[1]),
                "sputnik": clean_count(values[4]),
                "pfizer": clean_count(values[6]),
                "moderna": clean_count(values[9].split(" ")[1]),
            },
            "third": {"pfizer": clean_count(values[8])},
        }

        people_vaccinated = sum(doses["first"].values())
        people_fully_vaccinated = sum(doses["second"].values())
        total_boosters = sum(doses["third"].values())
        total_vaccinations = people_vaccinated + people_fully_vaccinated + total_boosters

        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_boosters": total_boosters,
            }
        )

    def export(self):
        data = self.read()
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    SriLanka().export()


if __name__ == "__main__":
    main()
