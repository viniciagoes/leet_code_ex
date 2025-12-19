import pandas as pd

data = [[900001, "Alice"], [900002, "Bob"], [900003, "Charlie"]]
users = pd.DataFrame(data, columns=["account", "name"]).astype(
    {"account": "Int64", "name": "object"}
)
data = [
    [1, 900001, 7000, "2020-08-01"],
    [2, 900001, 7000, "2020-09-01"],
    [3, 900001, -3000, "2020-09-02"],
    [4, 900002, 1000, "2020-09-12"],
    [5, 900003, 6000, "2020-08-07"],
    [6, 900003, 6000, "2020-09-07"],
    [7, 900003, -4000, "2020-09-11"],
]
transactions = pd.DataFrame(
    data, columns=["trans_id", "account", "amount", "transacted_on"]
).astype(
    {
        "trans_id": "Int64",
        "account": "Int64",
        "amount": "Int64",
        "transacted_on": "datetime64[ns]",
    }
)


def account_summary(users: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    df = transactions.merge(users, on="account")

    df = df.groupby("name").agg({"amount": "sum"}).reset_index()

    return df[df["amount"] > 10000].rename(columns={"amount": "balance"})
