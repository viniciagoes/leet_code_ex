import pandas as pd

data = [
    [6, "2020-06-30 15:06:07"],
    [6, "2021-04-21 14:06:06"],
    [6, "2019-03-07 00:18:15"],
    [8, "2020-02-01 05:10:53"],
    [8, "2020-12-30 00:46:50"],
    [2, "2020-01-16 02:49:50"],
    [2, "2019-08-25 07:59:08"],
    [14, "2019-07-14 09:00:00"],
    [14, "2021-01-06 11:59:59"],
]
logins = pd.DataFrame(data, columns=["user_id", "time_stamp"]).astype(
    {"user_id": "Int64", "time_stamp": "datetime64[ns]"}
)


def latest_login(logins: pd.DataFrame) -> pd.DataFrame:
    df = logins[logins["time_stamp"].dt.year == 2020]
    df = df.groupby("user_id")["time_stamp"].max().reset_index()
    df.rename(columns={"time_stamp": "last_stamp"}, inplace=True)

    return df


df = latest_login(logins)
df.head()
