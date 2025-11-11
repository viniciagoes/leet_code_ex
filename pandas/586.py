import pandas as pd

data = [[1, 1], [2, 2], [3, 3], [4, 3]]
orders = pd.DataFrame(data, columns=["order_number", "customer_number"]).astype(
    {"order_number": "Int64", "customer_number": "Int64"}
)


def largest_orders(orders: pd.DataFrame) -> pd.DataFrame:
    df = orders[["customer_number"]].value_counts().reset_index()

    max_orders = df.max()
    max_n = int(max_orders.iloc[1])

    df = df[df["count"] == max_n]

    return df[["customer_number"]]


df = largest_orders(orders)
print(df.head())
