import pandas as pd

data = [
    [1, 2, "2016-03-01", 5],
    [1, 2, "2016-03-02", 6],
    [2, 3, "2017-06-25", 1],
    [3, 1, "2016-03-02", 0],
    [3, 4, "2018-07-03", 5],
]
activity = pd.DataFrame(
    data, columns=["player_id", "device_id", "event_date", "games_played"]
).astype(
    {
        "player_id": "Int64",
        "device_id": "Int64",
        "event_date": "datetime64[ns]",
        "games_played": "Int64",
    }
)


def gameplay_analysis(activity: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "fraction": [
                (
                    round(
                        activity[
                            activity.event_date
                            == activity.groupby("player_id").event_date.transform(min)
                            + pd.Timedelta(days=1)
                        ].shape[0]
                        / activity.player_id.nunique(),
                        2,
                    )
                )
            ]
        }
    )
