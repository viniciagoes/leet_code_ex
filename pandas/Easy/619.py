import pandas as pd

data = [[8], [8], [7], [7], [3], [3], [3]]
my_numbers = pd.DataFrame(data, columns=['num']).astype({'num':'Int64'})

def biggest_single_number(my_numbers: pd.DataFrame) -> pd.DataFrame:
    df = my_numbers.value_counts("num").reset_index()

    df = df[df["count"] == 1]
    if df.empty:
        return pd.DataFrame({"num": pd.Series([pd.NA], dtype="Int64")})
    
    df = df[df["num"] == df["num"].max()][["num"]]

    return df

df = biggest_single_number(my_numbers)
print(df)