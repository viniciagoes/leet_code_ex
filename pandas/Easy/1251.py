import pandas as pd

data = [
    [1, "2019-02-17", "2019-02-28", 5],
    [1, "2019-03-01", "2019-03-22", 20],
    [2, "2019-02-01", "2019-02-20", 15],
    [2, "2019-02-21", "2019-03-31", 30],
]
prices = pd.DataFrame(
    data, columns=["product_id", "start_date", "end_date", "price"]
).astype(
    {
        "product_id": "Int64",
        "start_date": "datetime64[ns]",
        "end_date": "datetime64[ns]",
        "price": "Int64",
    }
)
data = [
    [1, "2019-02-25", 100],
    [1, "2019-03-01", 15],
    [2, "2019-02-10", 200],
    [2, "2019-03-22", 30],
]
units_sold = pd.DataFrame(
    data, columns=["product_id", "purchase_date", "units"]
).astype({"product_id": "Int64", "purchase_date": "datetime64[ns]", "units": "Int64"})

def average_selling_price(
    prices: pd.DataFrame, units_sold: pd.DataFrame
) -> pd.DataFrame:
    # join and fix join...
    df = prices.merge(units_sold, on="product_id", how="left")
    df = df[
        (df["purchase_date"].isna())
        | (
            (df["purchase_date"] >= df["start_date"])
            & (df["purchase_date"] <= df["end_date"])
        )
    ]

    df["units"] = df["units"].fillna(0)
    df["cost"] = df["price"] * df["units"]

    df = df.groupby("product_id").agg({"cost": "sum", "units": "sum"}).reset_index()
    df["average_price"] = (df["cost"] / df["units"].replace(0, 1)).round(2)

    df["average_price"] = df["average_price"].fillna(0)

    return df[["product_id", "average_price"]]


df = average_selling_price(prices, units_sold)
print(df.head())
