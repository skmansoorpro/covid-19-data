import json
import requests

import pandas as pd

from cowidev.utils import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.testing.utils.base import CountryTestBase


class Jordan(CountryTestBase):
    location: str = "Jordan"
    units: str = "tests performed"
    source_label: str = "Ministry of Health"
    date: str = localdate("Asia/Amman")
    notes: str = ""
    source_url: str = (
        "https://wabi-west-europe-d-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"
    )
    source_url_ref: str = "https://corona.moh.gov.jo/ar"

    def read(self) -> pd.DataFrame:
        """Reads the data from the source"""
        data = self.data_body
        try:
            count = json.loads(requests.post(self.source_url, headers=self.headers, data=json.dumps(data)).content)[
                "results"
            ][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0]["DM0"][0]["M0"]
            return self._df_builder(count)
        except KeyError:
            raise KeyError("No value found. Please check the date.")

    @property
    def headers(Self):
        """Headers for the request"""
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:85.0) Gecko/20100101 Firefox/85.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US",
            "X-PowerBI-ResourceKey": "1f83a65b-cea7-48df-8be5-85840c51f971",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://app.powerbi.com",
            "Referer": "https://app.powerbi.com/",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    @property
    def data_body(self):
        """Request payload"""
        data = {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [{"Name": "m", "Entity": "Main dashboard", "Type": 0}],
                                        "Select": [
                                            {
                                                "Aggregation": {
                                                    "Expression": {
                                                        "Column": {
                                                            "Expression": {"SourceRef": {"Source": "m"}},
                                                            "Property": "Total Tests",
                                                        }
                                                    },
                                                    "Function": 0,
                                                },
                                                "Name": "CountNonNull(Main dashboard.Total Tests)",
                                            }
                                        ],
                                        "Where": [
                                            {
                                                "Condition": {
                                                    "In": {
                                                        "Expressions": [
                                                            {
                                                                "Column": {
                                                                    "Expression": {"SourceRef": {"Source": "m"}},
                                                                    "Property": "Date",
                                                                }
                                                            }
                                                        ],
                                                        "Values": [
                                                            [{"Literal": {"Value": f"datetime'{self.date}T00:00:00'"}}]
                                                        ],
                                                    }
                                                }
                                            }
                                        ],
                                    },
                                    "Binding": {"Primary": {"Groupings": [{"Projections": [0]}]}, "Version": 1},
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ]
                    },
                    "QueryId": "",
                    "ApplicationContext": {"DatasetId": "e856a62e-c5b7-4866-bce5-0faf7154e2e5"},
                }
            ],
            "cancelQueries": [],
            "modelId": 1084599,
        }
        return data

    def _df_builder(self, count: str) -> pd.DataFrame:
        """Builds dataframe from the text data"""
        df = pd.DataFrame({"Cumulative total": [clean_count(count)]})
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipes date."""
        return df.assign(Date=self.date)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pipeline for data."""
        return df.pipe(self.pipe_date).pipe(self.pipe_metadata)

    def export(self):
        """Exports data to CSV."""
        df = self.read().pipe(self.pipeline)
        # Export to CSV
        self.export_datafile(df, attach=True)


def main():
    Jordan().export()
