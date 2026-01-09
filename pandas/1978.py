import pandas as pd

data = [
    [3, "Mila", 9, 60301],
    [12, "Antonella", None, 31000],
    [13, "Emery", None, 67084],
    [1, "Kalel", 11, 21241],
    [9, "Mikaela", None, 50937],
    [11, "Joziah", 6, 28485],
]
employees = pd.DataFrame(
    data, columns=["employee_id", "name", "manager_id", "salary"]
).astype(
    {"employee_id": "Int64", "name": "object", "manager_id": "Int64", "salary": "Int64"}
)


def find_employees(employees: pd.DataFrame) -> pd.DataFrame:
    df = employees.merge(
        employees, left_on="manager_id", right_on="employee_id", how="left"
    )

    df = df[
        (df["salary_x"] < 30000)
        & (~df["manager_id_x"].isna())
        & (df["employee_id_y"].isna())
    ].loc[:, ["employee_id_x"]]

    df.sort_values("employee_id_x", inplace=True)

    return df.rename(columns={"employee_id_x": "employee_id"})


df = find_employees(employees)
df.head()
