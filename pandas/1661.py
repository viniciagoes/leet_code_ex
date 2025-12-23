import pandas as pd

data = [
    [0, 0, "start", 0.712],
    [0, 0, "end", 1.52],
    [0, 1, "start", 3.14],
    [0, 1, "end", 4.12],
    [1, 0, "start", 0.55],
    [1, 0, "end", 1.55],
    [1, 1, "start", 0.43],
    [1, 1, "end", 1.42],
    [2, 0, "start", 4.1],
    [2, 0, "end", 4.512],
    [2, 1, "start", 2.5],
    [2, 1, "end", 5],
]
activity = pd.DataFrame(
    data, columns=["machine_id", "process_id", "activity_type", "timestamp"]
).astype(
    {
        "machine_id": "Int64",
        "process_id": "Int64",
        "activity_type": "object",
        "timestamp": "Float64",
    }
)


def get_average_time(activity: pd.DataFrame) -> pd.DataFrame:
    df1 = activity[activity["activity_type"] == "start"]
    df2 = activity[activity["activity_type"] == "end"]

    df = df1.merge(df2, on=["machine_id", "process_id"])[
        ["machine_id", "process_id", "timestamp_y", "timestamp_x"]
    ]
    df["time"] = df["timestamp_y"] - df["timestamp_x"]

    df = df.groupby("machine_id")["time"].mean().round(3).reset_index()

    df.rename(columns={"time": "processing_time"}, inplace=True)

    return df
