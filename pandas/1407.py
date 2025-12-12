import pandas as pd

data = [
    [1, "Alice"],
    [2, "Bob"],
    [3, "Alex"],
    [4, "Donald"],
    [7, "Lee"],
    [13, "Jonathan"],
    [19, "Elvis"],
]
users = pd.DataFrame(data, columns=["id", "name"]).astype(
    {"id": "Int64", "name": "object"}
)
data = [
    [1, 1, 120],
    [2, 2, 317],
    [3, 3, 222],
    [4, 7, 100],
    [5, 13, 312],
    [6, 19, 50],
    [7, 7, 120],
    [8, 19, 400],
    [9, 7, 230],
]
rides = pd.DataFrame(data, columns=["id", "user_id", "distance"]).astype(
    {"id": "Int64", "user_id": "Int64", "distance": "Int64"}
)

df = users.merge(
    rides, left_on="id", right_on="user_id", how="left", suffixes=("", "_r")
)[["id", "name", "distance"]].fillna(0)
df.head(30)

df = df.groupby(["id", "name"])["distance"].sum().reset_index()[["name", "distance"]]
df.head()

df.sort_values(by=["distance", "name"], ascending=[False, True], inplace=True)
df.head(30)

df.rename(columns={"distance": "travelled_distance"}, inplace=True)
df.head(30)
# def top_travellers(users: pd.DataFrame, rides: pd.DataFrame) -> pd.DataFrame:
