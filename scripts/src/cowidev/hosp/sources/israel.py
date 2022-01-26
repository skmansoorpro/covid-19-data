import pandas as pd


METADATA = {
    "source_url": "https://raw.githubusercontent.com/yuval-harpaz/covid-19-israel-matlab/master/data/Israel/dashboard_timeseries.csv",
    "source_url_ref": "https://datadashboard.health.gov.il/COVID-19/",
    "source_name": "Ministry of Health, via Yuval Harpaz on GitHub",
    "entity": "Israel",
}


def main():

    df = pd.read_csv(
        METADATA["source_url"],
        usecols=["date", "new_hospitalized", "CountHospitalized", "serious_critical_new", "CountCriticalStatus"],
    )

    df["date"] = pd.to_datetime(df.date, format="%d-%b-%Y").dt.date.astype(str)

    df = df.sort_values("date")

    df["new_hospitalized"] = df.new_hospitalized.rolling(7).sum()
    df["serious_critical_new"] = df.serious_critical_new.rolling(7).sum()

    df.loc[df.date < "2020-09-01", "CountCriticalStatus"] = pd.NA

    df = df.melt("date", var_name="indicator").dropna(subset=["value"]).sort_values("date").head(-1)
    df["indicator"] = df.indicator.replace(
        {
            "CountHospitalized": "Daily hospital occupancy",
            "CountCriticalStatus": "Daily ICU occupancy",
            "serious_critical_new": "Weekly new ICU admissions",
            "new_hospitalized": "Weekly new hospital admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
