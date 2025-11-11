import pandas as pd

data = [
    [1, "Will", None],
    [2, "Jane", None],
    [3, "Alex", 2],
    [4, "Bill", None],
    [5, "Zack", 1],
    [6, "Mark", 2],
]
customer = pd.DataFrame(data, columns=["id", "name", "referee_id"]).astype(
    {"id": "Int64", "name": "object", "referee_id": "Int64"}
)


def find_customer_referee(customer: pd.DataFrame) -> pd.DataFrame:
    return customer[(customer["referee_id"].isnull()) | (customer["referee_id"] != 2)][
        ["name"]
    ]


df = find_customer_referee(customer)
print(df.head())
