import pandas as pd

data = [[1, 100], [2, 200], [3, 300]]
employee = pd.DataFrame(data, columns=["Id", "Salary"]).astype(
    {"Id": "Int64", "Salary": "Int64"}
)


def nth_highest_salary(employee: pd.DataFrame, N: int) -> pd.DataFrame:
    if N < 1:
        return pd.DataFrame([[None]], columns=[f"getNthHighestSalary({N})"]).astype(
            {f"getNthHighestSalary({N})": "Int64"}
        )

    df = (
        employee.drop_duplicates(subset=["salary"])
        .sort_values(by="salary", ascending=False)
        .rename(columns={"salary": f"getNthHighestSalary({N})"})
        .iloc[N - 1 : N, [1]]
    )

    return (
        df
        if not df.empty
        else pd.DataFrame([[None]], columns=[f"getNthHighestSalary({N})"]).astype(
            {f"getNthHighestSalary({N})": "Int64"}
        )
    )


df = nth_highest_salary(employee, N=1)
df.head()
