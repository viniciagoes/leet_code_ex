import pandas as pd

data = [
    [1, 2, "2016/06/03"],
    [1, 3, "2016/06/08"],
    [2, 3, "2016/06/08"],
    [3, 4, "2016/06/09"],
]
request_accepted = pd.DataFrame(
    data, columns=["requester_id", "accepter_id", "accept_date"]
).astype(
    {"requester_id": "Int64", "accepter_id": "Int64", "accept_date": "datetime64[ns]"}
)


def most_friends(request_accepted: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat(
        [
            request_accepted.rename(columns={"requester_id": "id"}).loc[:, ["id"]],
            request_accepted.rename(columns={"accepter_id": "id"}).loc[:, ["id"]],
        ],
        ignore_index=True,
    )

    df = df.value_counts().reset_index()

    return df.rename(columns={"count": "num"}).loc[:0]


df = most_friends(request_accepted)
df.head()
