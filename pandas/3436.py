import pandas as pd

data = [
    [1, "alice@example.com"],
    [2, "bob_at_example.com"],
    [3, "charlie@example.net"],
    [4, "david@domain.com"],
    [5, "eve@invalid"],
]
users = pd.DataFrame(data, columns=["user_id", "email"]).astype(
    {"user_id": "int32", "email": "string"}
)
users.head()


def find_valid_emails(users: pd.DataFrame) -> pd.DataFrame:
    df = users[users["email"].str.match(r"^[a-zA-Z0-9_]+@[a-zA-Z]+\.com$")]

    return df.sort_values("user_id")


df = find_valid_emails(users)
df.head()
