import pandas as pd

data = [
    [1, "Joe", 70000, 1],
    [2, "Jim", 90000, 1],
    [3, "Henry", 80000, 2],
    [4, "Sam", 60000, 2],
    [5, "Max", 90000, 1],
]
employee = pd.DataFrame(data, columns=["id", "name", "salary", "departmentId"]).astype(
    {"id": "Int64", "name": "object", "salary": "Int64", "departmentId": "Int64"}
)
data = [[1, "IT"], [2, "Sales"]]
department = pd.DataFrame(data, columns=["id", "name"]).astype(
    {"id": "Int64", "name": "object"}
)


def department_highest_salary(
    employee: pd.DataFrame, department: pd.DataFrame
) -> pd.DataFrame:
    df = employee.merge(department, left_on="departmentId", right_on="id")

    df["rank"] = df.groupby("departmentId")["salary"].rank(
        method="dense", ascending=False
    )

    df = df[df["rank"] == 1].loc[:, ["name_y", "name_x", "salary"]]

    return df.rename(
        columns={"name_y": "Department", "name_x": "Employee", "salary": "Salary"},
    )


df = department_highest_salary(employee, department)
df.head()
