import pandas as pd

data = [[1, 5], [2, 6], [3, 5], [3, 6], [1, 6]]
customer = pd.DataFrame(data, columns=["customer_id", "product_key"]).astype(
    {"customer_id": "Int64", "product_key": "Int64"}
)
data = [[5], [6]]
product = pd.DataFrame(data, columns=["product_key"]).astype({"product_key": "Int64"})


def find_customers(customer: pd.DataFrame, product: pd.DataFrame) -> pd.DataFrame:
    df = customer.groupby("customer_id")["product_key"].nunique().reset_index()
    df = df[df["product_key"] == product["product_key"].nunique()].loc[
        :, ["customer_id"]
    ]

    return df


df = find_customers(customer, product)
df.head()
