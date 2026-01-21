import pandas as pd

data = [[1, 1], [2, 1], [3, 1], [4, 2], [5, 1], [6, 2], [7, 2]]
logs = pd.DataFrame(data, columns=["id", "num"]).astype({"id": "Int64", "num": "Int64"})


def consecutive_numbers(logs: pd.DataFrame) -> pd.DataFrame:
    df = logs.sort_values("id").merge(
        logs.sort_values("id").shift(1),
        left_index=True,
        right_index=True,
        suffixes=("", "_last"),
    )

    df = df.merge(
        logs.sort_values("id").shift(-1),
        left_index=True,
        right_index=True,
        suffixes=("", "_next"),
    )

    return (
        df[
            (df["num"] == df["num_last"])
            & (df["num"] == df["num_next"])
            & (df["id"] == df["id_last"] + 1)
            & (df["id"] == df["id_next"] - 1)
        ]
        .loc[:, ["num"]]
        .drop_duplicates()
        .rename(columns={"num": "ConsecutiveNums"})
    )


df = consecutive_numbers(logs)
df.head()
