import pandas as pd

data = [
    [1, 20, "2019-08-14"],
    [2, 50, "2019-08-14"],
    [1, 30, "2019-08-15"],
    [1, 35, "2019-08-16"],
    [2, 65, "2019-08-17"],
    [3, 20, "2019-08-18"],
]
products = pd.DataFrame(
    data, columns=["product_id", "new_price", "change_date"]
).astype({"product_id": "Int64", "new_price": "Int64", "change_date": "datetime64[ns]"})


def price_at_given_date(products: pd.DataFrame) -> pd.DataFrame:
    std_values = products.groupby("product_id")["change_date"].min().reset_index()
    std_values = std_values[std_values["change_date"] > "2019-08-16"].loc[
        :, ["product_id"]
    ]
    std_values["price"] = 10

    valid_values = (
        products[products["change_date"] <= "2019-08-16"]
        .groupby("product_id")["change_date"]
        .max()
        .reset_index()
    )
    valid_values = products.merge(valid_values, on=["change_date", "product_id"]).loc[
        :, ["product_id", "new_price"]
    ]
    valid_values = valid_values.rename(columns={"new_price": "price"})

    return pd.concat([std_values, valid_values])
