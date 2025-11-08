import pandas as pd

def find_employees(employee: pd.DataFrame) -> pd.DataFrame:
    merg = employee.merge(
        employee,
        on="id",
        how="left",
        suffixes="manager"
    )

    return merg

    
