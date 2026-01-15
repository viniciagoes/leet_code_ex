import pandas as pd

data = [
    [1, 8000, "Jan"],
    [2, 9000, "Jan"],
    [3, 10000, "Feb"],
    [1, 7000, "Feb"],
    [1, 6000, "Mar"],
]
department = pd.DataFrame(data, columns=["id", "revenue", "month"]).astype(
    {"id": "Int64", "revenue": "Int64", "month": "object"}
)


def reformat_table(department: pd.DataFrame) -> pd.DataFrame:
    bymonth = department.pivot(index="id", columns="month", values="revenue")
    bymonth = bymonth.reindex(
        columns=[
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
    )
    bymonth.rename(columns=lambda prefix: prefix + "_Revenue", inplace=True)
    bymonth.reset_index(inplace=True)

    return bymonth


df = reformat_table(department)
print(df.head())
