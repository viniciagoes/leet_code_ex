import pandas as pd

data = [
    [1, "2018-01-01", "Lenovo"],
    [2, "2018-02-09", "Samsung"],
    [3, "2018-01-19", "LG"],
    [4, "2018-05-21", "HP"],
]
users = pd.DataFrame(data, columns=["user_id", "join_date", "favorite_brand"]).astype(
    {"user_id": "Int64", "join_date": "datetime64[ns]", "favorite_brand": "object"}
)
data = [
    [1, "2019-08-01", 4, 1, 2],
    [2, "2018-08-02", 2, 1, 3],
    [3, "2019-08-03", 3, 2, 3],
    [4, "2018-08-04", 1, 4, 2],
    [5, "2018-08-04", 1, 3, 4],
    [6, "2019-08-05", 2, 2, 4],
]
orders = pd.DataFrame(
    data, columns=["order_id", "order_date", "item_id", "buyer_id", "seller_id"]
).astype(
    {
        "order_id": "Int64",
        "order_date": "datetime64[ns]",
        "item_id": "Int64",
        "buyer_id": "Int64",
        "seller_id": "Int64",
    }
)
data = [[1, "Samsung"], [2, "Lenovo"], [3, "LG"], [4, "HP"]]
items = pd.DataFrame(data, columns=["item_id", "item_brand"]).astype(
    {"item_id": "Int64", "item_brand": "object"}
)


def market_analysis(
    users: pd.DataFrame, orders: pd.DataFrame, items: pd.DataFrame
) -> pd.DataFrame:

    df = (
        users.merge(
            orders[orders["order_date"].dt.year == 2019]
            .groupby("buyer_id")["order_id"]
            .count()
            .reset_index(),
            left_on="user_id",
            right_on="buyer_id",
            how="left",
        )
        .loc[:, ["user_id", "join_date", "order_id"]]
        .rename(columns={"user_id": "buyer_id", "order_id": "orders_in_2019"})
        .fillna(0)
    )

    return df


df = market_analysis(users, orders, items)
df.head()
