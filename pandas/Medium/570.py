import pandas as pd

data = [
    [101, "John", "A", None],
    [102, "Dan", "A", 101],
    [103, "James", "A", 101],
    [104, "Amy", "A", 101],
    [105, "Anne", "A", 101],
    [106, "Ron", "B", 101],
]
employee = pd.DataFrame(data, columns=["id", "name", "department", "managerId"]).astype(
    {"id": "Int64", "name": "object", "department": "object", "managerId": "Int64"}
)


def find_managers(employee: pd.DataFrame) -> pd.DataFrame:
    top_managers = employee.groupby("managerId")["managerId"].count()
    manager_list = top_managers[top_managers >= 5].index.tolist()

    return employee[employee["id"].isin(manager_list)].loc[:, ["name"]]


df = find_managers(employee)
df.head()
