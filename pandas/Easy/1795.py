import pandas as pd

data = [[0, 95, 100, 105], [1, 70, None, 80]]
products = pd.DataFrame(
    data, columns=["product_id", "store1", "store2", "store3"]
).astype(
    {"product_id": "Int64", "store1": "Int64", "store2": "Int64", "store3": "Int64"}
)


def rearrange_products_table(products: pd.DataFrame) -> pd.DataFrame:
    df = products.melt(
        id_vars=["product_id"],
        var_name="store",
        value_vars=["store1", "store2", "store3"],
        value_name="price",
    )

    df.dropna(inplace=True)
    df.sort_values(by="product_id", inplace=True)

    return df
