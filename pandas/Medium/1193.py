import pandas as pd

data = [
    [121, "US", "approved", 1000, "2018-12-18"],
    [122, "US", "declined", 2000, "2018-12-19"],
    [123, "US", "approved", 2000, "2019-01-01"],
    [124, "DE", "approved", 2000, "2019-01-07"],
    [125, None, "declined", 2000, "2019-01-07"],
]
transactions = pd.DataFrame(
    data, columns=["id", "country", "state", "amount", "trans_date"]
).astype(
    {
        "id": "Int64",
        "country": "object",
        "state": "object",
        "amount": "Int64",
        "trans_date": "datetime64[ns]",
    }
)


def monthly_transactions(transactions: pd.DataFrame) -> pd.DataFrame:

    transactions["month"] = transactions["trans_date"].dt.strftime("%Y-%m")

    approved = (
        transactions[transactions["state"] == "approved"]
        .groupby(["country", "month"], dropna=False)
        .agg({"id": "count", "amount": "sum"})
        .reset_index()
    )

    all = (
        transactions.groupby(["country", "month"], dropna=False)
        .agg({"id": "count", "amount": "sum"})
        .reset_index()
    )

    df = all.merge(approved, how="left", on=["country", "month"])
    df = df.rename(
        columns={
            "id_x": "trans_count",
            "amount_x": "trans_total_amount",
            "id_y": "approved_count",
            "amount_y": "approved_total_amount",
        }
    )

    df["approved_count"] = df["approved_count"].fillna(0)
    df["approved_total_amount"] = df["approved_total_amount"].fillna(0)
    df["trans_count"] = df["trans_count"].fillna(0)
    df["trans_total_amount"] = df["trans_total_amount"].fillna(0)

    return df.loc[
        :,
        [
            "month",
            "country",
            "trans_count",
            "approved_count",
            "trans_total_amount",
            "approved_total_amount",
        ],
    ]


df = monthly_transactions(transactions)
df.head()
