import pandas as pd

data = [[1, "john@example.com"], [2, "bob@example.com"], [3, "john@example.com"]]
person = pd.DataFrame(data, columns=["id", "email"]).astype(
    {"id": "int64", "email": "object"}
)


def delete_duplicate_emails(person: pd.DataFrame) -> None:
    person.sort_values("id", ascending=True, inplace=True)
    person.drop_duplicates(subset=["email"], keep="first", inplace=True)


# df["rn"] = df.sort_values("id", ascending=True) \
#             .groupby("email") \
#             .cumcount()

# df = df[["id", "email"]].where(df["rn"] == 0).dropna()
