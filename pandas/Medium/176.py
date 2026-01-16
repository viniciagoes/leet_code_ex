import pandas as pd

data = [[1, 100], [2, 200], [3, 300]]
employee = pd.DataFrame(data, columns=["id", "salary"]).astype(
    {"id": "int64", "salary": "int64"}
)


def second_highest_salary(employee: pd.DataFrame) -> pd.DataFrame:
    first_max = employee["salary"].max()
    df = employee[employee["salary"] < first_max]
    df = df.sort_values("salary", ascending=False).head(1)

    return (
        df.rename(columns={"salary": "SecondHighestSalary"})
        if not df.empty
        else pd.DataFrame({"SecondHighestSalary": [None]})
    )


df = second_highest_salary(employee)
df.head()
