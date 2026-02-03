import pandas as pd

data = [[1, "Abbot"], [2, "Doris"], [3, "Emerson"], [4, "Green"], [5, "Jeames"]]
seat = pd.DataFrame(data, columns=["id", "student"]).astype(
    {"id": "Int64", "student": "object"}
)


def exchange_seats(seat: pd.DataFrame) -> pd.DataFrame:
    seat["new_id"] = seat["id"].case_when(
        [(seat["id"] % 2 == 0, seat["id"] - 1), (seat["id"] % 2 == 1, seat["id"] + 1)]
    )

    seat = seat.merge(seat, left_on="new_id", right_on="id", how="left")

    df = seat.loc[:, ["id_x", "student_x", "student_y"]]

    df["student"] = df["student_y"].fillna(df["student_x"])

    return df.rename(columns={"id_x": "id"}).loc[:, ["id", "student"]]


df = exchange_seats(seat)
df.head()
