import pandas as pd

data = [[1, 1], [1, 2], [1, 3], [2, 1], [2, 4]]
project = pd.DataFrame(data, columns=["project_id", "employee_id"]).astype(
    {"project_id": "Int64", "employee_id": "Int64"}
)
data = [[1, "Khaled", 3], [2, "Ali", 2], [3, "John", 1], [4, "Doe", 2]]
employee = pd.DataFrame(
    data, columns=["employee_id", "name", "experience_years"]
).astype({"employee_id": "Int64", "name": "object", "experience_years": "Int64"})


def project_employees_i(project: pd.DataFrame, employee: pd.DataFrame) -> pd.DataFrame:
    df = project.merge(employee, on="employee_id")

    df = (
        df.groupby("project_id")["experience_years"]
        .mean()
        .round(decimals=2)
        .rename("average_years")
        .reset_index()
    )

    return df


df = project_employees_i(project, employee)
print(df.head())
