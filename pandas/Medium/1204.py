import pandas as pd

data = [
    [5, "Alice", 250, 1],
    [4, "Bob", 175, 5],
    [3, "Alex", 350, 2],
    [6, "John Cena", 400, 3],
    [1, "Winston", 500, 6],
    [2, "Marie", 200, 4],
]
queue = pd.DataFrame(
    data, columns=["person_id", "person_name", "weight", "turn"]
).astype(
    {"person_id": "Int64", "person_name": "object", "weight": "Int64", "turn": "Int64"}
)


def last_passenger(queue: pd.DataFrame) -> pd.DataFrame:
    df = queue.sort_values("turn")["weight"].cumsum()

    df = df[df <= 1000].sort_values(ascending=False).head(1)

    return queue.merge(df, left_index=True, right_index=True).loc[:, ["person_name"]]
