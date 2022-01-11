from datetime import datetime

import pandas as pd
from cowidev.utils.web import request_json


class AfricaCDC:
    def __init__(self) -> None:
        self._base_url = (
            "https://services8.arcgis.com/vWozsma9VzGndzx7/ArcGIS/rest/services/"
            "DailyCOVIDDashboard_5July21_1/FeatureServer/0/"
        )

        self.source_url_ref = "https://africacdc.org/covid-19-vaccination/"

    @property
    def source_url(self):
        return f"{self.base_url}/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=Country%2CTests_Conducted%2CDate&returnGeometry=false&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token="

    @property
    def source_url_date(self):
        return f"{self._base_url}?f=pjson"

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        res = [d["attributes"] for d in data["features"]]

        df = pd.DataFrame(
            res,
            columns=[
                "Country",
                "Tests_Conducted",
                "Date",
            ],
        )
        return df

    def pipe_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "Country": "location",
                "Tests_Conducted": "Cumulative total",
                "Date": "Date",
            }
        )
