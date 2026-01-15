import pandas as pd

data = [[1, "aLice"], [2, "bOB"]]
users = pd.DataFrame(data, columns=["user_id", "name"]).astype(
    {"user_id": "Int64", "name": "object"}
)


def fix_names(users: pd.DataFrame) -> pd.DataFrame:
    users["name"] = users["name"].str.lower().str.capitalize()
    return users.sort_values("user_id")
