import pandas as pd

data = [
    [1, "Jhon", "2019-01-01", 100],
    [2, "Daniel", "2019-01-02", 110],
    [3, "Jade", "2019-01-03", 120],
    [4, "Khaled", "2019-01-04", 130],
    [5, "Winston", "2019-01-05", 110],
    [6, "Elvis", "2019-01-06", 140],
    [7, "Anna", "2019-01-07", 150],
    [8, "Maria", "2019-01-08", 80],
    [9, "Jaze", "2019-01-09", 110],
    [1, "Jhon", "2019-01-10", 130],
    [3, "Jade", "2019-01-10", 150],
]
customer = pd.DataFrame(
    data, columns=["customer_id", "name", "visited_on", "amount"]
).astype(
    {
        "customer_id": "Int64",
        "name": "object",
        "visited_on": "datetime64[ns]",
        "amount": "Int64",
    }
)


def restaurant_growth(customer: pd.DataFrame) -> pd.DataFrame:

    df = customer.groupby("visited_on", as_index=False)["amount"].sum()

    df.amount = df.amount.rolling(7).sum()
    df["average_amount"] = df.amount.div(7)

    return df.dropna().round(2)


df = restaurant_growth(customer)
df.head()
