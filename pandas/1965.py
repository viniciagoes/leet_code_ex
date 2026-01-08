import pandas as pd

data = [[2, 'Crew'], [4, 'Haven'], [5, 'Kristian']]
employees = pd.DataFrame(data, columns=['employee_id', 'name']).astype({'employee_id':'Int64', 'name':'object'})
data = [[5, 76071], [1, 22517], [4, 63539]]
salaries = pd.DataFrame(data, columns=['employee_id', 'salary']).astype({'employee_id':'Int64', 'salary':'Int64'})

def find_employees(employees: pd.DataFrame, salaries: pd.DataFrame) -> pd.DataFrame:
    df = employees.merge(
        salaries,
        how="outer",
        on="employee_id"
    )

    df = df[
        df["name"].isna() | df["salary"].isna()
        ].loc[:, ["employee_id"]]

    return df

df = find_employees(employees, salaries)
df.head()
