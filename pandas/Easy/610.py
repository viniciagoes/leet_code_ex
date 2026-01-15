import pandas as pd

data = [[13, 15, 30], [10, 20, 15]]
triangle = pd.DataFrame(data, columns=["x", "y", "z"]).astype(
    {"x": "Int64", "y": "Int64", "z": "Int64"}
)


def triangle_judgement(triangle: pd.DataFrame) -> pd.DataFrame:
    triangle["Triangle"] = (
        (triangle["x"] + triangle["y"] > triangle["z"])
        & (triangle["x"] + triangle["z"] > triangle["y"])
        & (triangle["y"] + triangle["z"] > triangle["x"])
    )

    triangle["Triangle"] = triangle["Triangle"].map({True: "Yes", False: "No"})

    return triangle


df = triangle_judgement(triangle)
print(df.head())
