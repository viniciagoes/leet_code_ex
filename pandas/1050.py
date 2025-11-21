import pandas as pd

data = [[1, 1, 0], [1, 1, 1], [1, 1, 2], [1, 2, 3], [1, 2, 4], [2, 1, 5], [2, 1, 6]]
actor_director = pd.DataFrame(
    data, columns=["actor_id", "director_id", "timestamp"]
).astype({"actor_id": "int64", "director_id": "int64", "timestamp": "int64"})


def actors_and_directors(actor_director: pd.DataFrame) -> pd.DataFrame:
    df = actor_director.value_counts(subset=["actor_id", "director_id"]).reset_index()

    return df[df["count"] >= 3][["actor_id", "director_id"]]


df = actors_and_directors(actor_director)
print(df.head())
