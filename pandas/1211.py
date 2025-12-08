import pandas as pd

data = [
    ["Dog", "Golden Retriever", 1, 5],
    ["Dog", "German Shepherd", 2, 5],
    ["Dog", "Mule", 200, 1],
    ["Cat", "Shirazi", 5, 2],
    ["Cat", "Siamese", 3, 3],
    ["Cat", "Sphynx", 7, 4],
]
queries = pd.DataFrame(
    data, columns=["query_name", "result", "position", "rating"]
).astype(
    {"query_name": "object", "result": "object", "position": "Int64", "rating": "Int64"}
)


def queries_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        quality=df.rating / df.position + 1e-10,
        poor_query_percentage=(df.rating < 3).astype(int) * 100,
    )
    return (
        df.groupby("query_name", as_index=False)[["quality", "poor_query_percentage"]]
        .mean()
        .round(2)
    )


df = queries_stats(queries)
print(df.head())
