import re

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd

from cowidev.utils.web import get_driver
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class Moldova:
    location = "Moldova"
    units = "tests performed"
    source_label = "Ministry of Health of the Republic of Moldova"
    notes = ""
    _source_url = "https://msmps.gov.md/minister/comunicare/comunicate/page/"
    _num_max_pages = 3
    regex = {
        "title": r"(cazuri noi de COVID-19)|(cazuri de COVID-19)|(cazuri de COVID-19,)",
        "date": r"(\d+\/\d+\/\d+)",
        "count": r"teste efectuate .*? (\d+)",
    }
    # Initial value for cumulative total: 364317

    def read(self) -> pd.Series:
        data = []
        with get_driver() as driver:
            for cnt in range(self._num_max_pages):
                url = f"{self._source_url}{cnt}"
                driver.get(url)
                data, proceed = self._parse_data(driver)
                if not proceed:
                    break

        return pd.Series(data)

    def _parse_data(self, driver: WebDriver) -> tuple:
        """Get data from the source page."""
        # Get relevant element
        elem = self._get_relevant_element(driver)
        if not elem:
            return None, True
        # Extract url and date from element
        url, date = self._get_link_and_date_from_element(elem)
        # Extract text from url
        text = self._get_text_from_url(url, driver)
        daily_change = self._parse_metrics(text)
        record = {
            "source_url": url,
            "date": date,
            "daily_change": daily_change,
        }
        return record, False

    def _get_relevant_element(self, driver: WebDriver) -> WebElement:
        """Get the relevant element in news feed."""
        news_list = driver.find_elements_by_class_name("list__news")
        url_idx = [
            i
            for i, news in enumerate(news_list)
            if re.search(
                self.regex["title"],
                news.find_element_by_class_name("h4").find_element_by_class_name("font__bigger").text,
            )
        ]

        if not url_idx:
            return None

        return news_list[url_idx[0]]

    def _get_text_from_url(self, url: str, driver: WebDriver) -> str:
        """Extract text from the url."""
        driver.get(url)
        text = (
            driver.find_element_by_class_name("editor")
            .text.replace("\n", " ")
            .replace("–", " ")
            .replace(".", "")
            .replace(",", "")
        )
        return text

    def _get_link_and_date_from_element(self, elem: WebElement) -> tuple:
        """Extract link and date from relevant element."""
        link = self._parse_link_from_element(elem)
        if not link:
            return None
        date = self._parse_date_from_element(elem)
        return link, date

    def _parse_date_from_element(self, elem: WebElement) -> str:
        """Get date from relevant element."""
        date = elem.find_element_by_class_name("list__post-date").text
        return clean_date(date, "%d/%m/%Y")

    def _parse_link_from_element(self, elem: WebElement) -> str:
        """Get link from relevant element."""
        href = elem.find_element_by_tag_name("a").get_attribute("href")
        return href

    def _parse_metrics(self, text: str) -> int:
        """Get metrics from news text."""
        count = int(re.search(self.regex["count"], text).group(1))
        return clean_count(count)

    def export(self):
        data = self.read()
        increment(
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=data["source_url"],
            source_label=self.source_label,
            daily_change=data["daily_change"],
            count=pd.NA,
        )


def main():
    Moldova().export()


if __name__ == "__main__":
    main()
