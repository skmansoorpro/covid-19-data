import re
from cowidev.testing.utils.base import CountryTestBase
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class Belize(CountryTestBase):
    location = "Belize"

    def export(self):
        data = pd.read_csv(self.output_path)

        op = Options()
        op.add_argument("--headless")

        url = "https://sib.org.bz/covid-19/by-the-numbers/"

        with webdriver.Chrome(options=op) as driver:
            driver.get(url)
            count = int(driver.find_element_by_class_name("stats-number").get_attribute("data-counter-value"))
            date = str(
                pd.to_datetime(
                    re.search(r"Last updated: (.*202\d)", driver.find_element_by_tag_name("body").text).group(1)
                ).date()
            )

        if count > data["Cumulative total"].max() and date > data["Date"].max():
            new = pd.DataFrame(
                {
                    "Country": self.location,
                    "Date": [date],
                    "Cumulative total": count,
                    "Source URL": url,
                    "Source label": "Statistical Institute of Belize",
                    "Units": "tests performed",
                }
            )
            df = pd.concat([new, data], sort=False).sort_values("Date")
            self.export_datafile(df)


def main():
    Belize().export()
