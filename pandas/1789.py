import pandas as pd

data = [
    ["1", "1", "N"],
    ["2", "1", "Y"],
    ["2", "2", "N"],
    ["3", "3", "N"],
    ["4", "2", "N"],
    ["4", "3", "Y"],
    ["4", "4", "N"],
]
employee = pd.DataFrame(
    data, columns=["employee_id", "department_id", "primary_flag"]
).astype({"employee_id": "Int64", "department_id": "Int64", "primary_flag": "object"})


def find_primary_department(employee: pd.DataFrame) -> pd.DataFrame:
    changed = employee.groupby("employee_id")["primary_flag"].count().reset_index()
    changed = changed[changed["primary_flag"] > 1]["employee_id"].tolist()

    df = employee[
        ((employee["primary_flag"] == "Y") & (employee["employee_id"].isin(changed)))
        | ((employee["primary_flag"] == "N") & (~employee["employee_id"].isin(changed)))
    ][["employee_id", "department_id"]]

    return df


df = find_primary_department(employee)
df.head()
