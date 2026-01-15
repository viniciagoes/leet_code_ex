import pandas as pd

data = [[1, 23], [2, 9], [4, 30], [5, 54], [6, 96], [7, 54], [8, 54]]
visits = pd.DataFrame(data, columns=["visit_id", "customer_id"]).astype(
    {"visit_id": "Int64", "customer_id": "Int64"}
)
data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
transactions = pd.DataFrame(
    data, columns=["transaction_id", "visit_id", "amount"]
).astype({"transaction_id": "Int64", "visit_id": "Int64", "amount": "Int64"})


def find_customers(visits: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    df = visits.merge(transactions, on="visit_id", how="left")

    df = (
        df[df["transaction_id"].isna()]
        .groupby("customer_id")["visit_id"]
        .nunique()
        .reset_index()
        .rename(columns={"visit_id": "count_no_trans"})
    )

    return df


df = find_customers(visits, transactions)
df.head(10)
