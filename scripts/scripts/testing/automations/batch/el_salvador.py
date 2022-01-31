import pandas as pd
from cowidev.testing import CountryTestBase
from cowidev.utils.web.scraping import get_response


class ElSalvador(CountryTestBase):
    location = "El Salvador"
    units = "tests performed"
    source_label = "Government of El Salvador"
    source_url = "https://diario.innovacion.gob.sv"
    source_url_ref = source_url
    token = ""

    def get_date(self, d):
        print(d)
        query_url = f"{self.source_url}/?_token={self.token}&fechaMostrar={d}"
        response = get_response(query_url)
        content = response.content
        data = (
            content.decode()
                .split("['Casos nuevos' , 'Pruebas', 'Recuperados', 'Fallecidos'],")[1]
                .split("[],")[0]
                .strip()
                .split("[")[1]
                .split("]")[0]
                .split(",")
        )
        data_dict = {"Date": d, "positive": data[0], "test": data[1]}
        return data_dict

    def read(self):
        response = get_response(self.source_url)
        content = response.content
        self.token = content.decode().split('name="_token" value="')[1].split('">')[0]
        dates = [
            s.strip()
            for s in content.decode()
                .split("var datesEnabled = [")[1]
                .split("];")[0]
                .replace("'", "")
                .replace("\n", "")
                .split(",")
            if len(s.strip()) > 0
        ]
        result = map(self.get_date, dates)
        return pd.DataFrame.from_records(result)

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")

        df["Daily change in cumulative total"] = df["test"]
        df["Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)

        df = df[df["Daily change in cumulative total"] != 0].reset_index()

        df["Positive rate"] = (
                df["positive"].rolling(7).sum() / df["Daily change in cumulative total"].rolling(7).sum()
        ).round(3)

        df["Positive rate"] = df["Positive rate"].fillna(0)
        df = df[["Date", "Daily change in cumulative total", "Positive rate", "positive"]]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_metrics).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        #self.export_datafile(df)
        df.to_csv(self.get_output_path(), index=False)

def main():
    ElSalvador().export()


if __name__ == "__main__":
    main()

