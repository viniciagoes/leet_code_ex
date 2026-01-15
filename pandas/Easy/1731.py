import pandas as pd

data = [
    [9, "Hercy", None, 43],
    [6, "Alice", 9, 41],
    [4, "Bob", 9, 36],
    [2, "Winston", None, 37],
]
employees = pd.DataFrame(
    data, columns=["employee_id", "name", "reports_to", "age"]
).astype(
    {"employee_id": "Int64", "name": "object", "reports_to": "Int64", "age": "Int64"}
)


def count_employees(employees: pd.DataFrame) -> pd.DataFrame:
    reports = employees.dropna(subset=["reports_to"])
    reports = (
        reports.groupby("reports_to")
        .agg({"employee_id": "count", "age": "mean"})
        .reset_index()
    )
    reports.rename(
        columns={
            "reports_to": "manager_id",
            "employee_id": "reports_count",
            "age": "average_age",
        },
        inplace=True,
    )
    reports.head()
    reports["average_age"] = (reports["average_age"] + 1e-12).round()
    reports.head()

    df = employees.merge(reports, left_on="employee_id", right_on="manager_id")

    df = df[["employee_id", "name", "reports_count", "average_age"]]
    df.sort_values(by="employee_id", inplace=True)

    return df


df = count_employees(employees)
df.head()
