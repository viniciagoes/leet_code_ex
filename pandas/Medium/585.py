import pandas as pd

data = [
    [1, 10, 5, 10, 10],
    [2, 20, 20, 20, 20],
    [3, 10, 30, 20, 20],
    [4, 10, 40, 40, 40],
]
insurance = pd.DataFrame(
    data, columns=["pid", "tiv_2015", "tiv_2016", "lat", "lon"]
).astype(
    {
        "pid": "Int64",
        "tiv_2015": "Float64",
        "tiv_2016": "Float64",
        "lat": "Float64",
        "lon": "Float64",
    }
)


def find_investments(insurance: pd.DataFrame) -> pd.DataFrame:
    crit_1 = insurance.groupby(["lat", "lon"])["pid"].count().reset_index()
    crit_1 = crit_1[crit_1["pid"] == 1]

    crit_2 = insurance.groupby("tiv_2015")["pid"].count().reset_index()
    crit_2 = crit_2[crit_2["pid"] > 1]

    df = insurance.merge(crit_1, on=["lat", "lon"]).merge(crit_2, on="tiv_2015")

    return pd.DataFrame(data=[df["tiv_2016"].sum().round(2)], columns=["tiv_2016"])
