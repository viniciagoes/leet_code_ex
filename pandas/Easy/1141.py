import pandas as pd

data = [
    [1, 1, "2019-07-20", "open_session"],
    [1, 1, "2019-07-20", "scroll_down"],
    [1, 1, "2019-07-20", "end_session"],
    [2, 4, "2019-07-20", "open_session"],
    [2, 4, "2019-07-21", "send_message"],
    [2, 4, "2019-07-21", "end_session"],
    [3, 2, "2019-07-21", "open_session"],
    [3, 2, "2019-07-21", "send_message"],
    [3, 2, "2019-07-21", "end_session"],
    [4, 3, "2019-06-25", "open_session"],
    [4, 3, "2019-06-25", "end_session"],
]
activity = pd.DataFrame(
    data, columns=["user_id", "session_id", "activity_date", "activity_type"]
).astype(
    {
        "user_id": "Int64",
        "session_id": "Int64",
        "activity_date": "datetime64[ns]",
        "activity_type": "object",
    }
)


def user_activity(activity: pd.DataFrame) -> pd.DataFrame:
    df = activity.where(
        (activity["activity_date"] <= pd.Timestamp("2019-07-27"))
        & (
            activity["activity_date"]
            > (pd.Timestamp("2019-07-27") - pd.DateOffset(days=30))
        )
    )

    df = df.groupby(by="activity_date")["user_id"].nunique().reset_index()

    return df.rename(columns={"activity_date": "day", "user_id": "active_users"})


df = user_activity(activity)
print(df.head())
