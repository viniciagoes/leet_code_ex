import pandas as pd

data = [
    [1, "Write a blog outline", 120],
    [1, "Generate SQL query", 80],
    [1, "Summarize an article", 200],
    [2, "Create resume bullet", 60],
    [2, "Improve LinkedIn bio", 70],
    [3, "Explain neural networks", 300],
    [3, "Generate interview Q&A", 250],
    [3, "Write cover letter", 180],
    [3, "Optimize Python code", 220],
]
prompts = pd.DataFrame(
    data,
    columns={
        "user_id": pd.Series(dtype="int"),
        "prompt": pd.Series(dtype="string"),
        "tokens": pd.Series(dtype="int"),
    },
)


def find_users_with_high_tokens(prompts: pd.DataFrame) -> pd.DataFrame:
    aggregations = (
        prompts.groupby("user_id")
        .agg({"prompt": "count", "tokens": "mean"})
        .round(2)
        .reset_index()
        .rename(columns={"prompt": "prompt_count", "tokens": "avg_tokens"})
    )

    aggregations.head()
    df = prompts.merge(aggregations, on="user_id")
    df.head()

    df = (
        df[(df["prompt_count"] >= 3) & (df["tokens"] > df["avg_tokens"])]
        .loc[:, ["user_id", "prompt_count", "avg_tokens"]]
        .drop_duplicates()
    )

    return df.sort_values(["avg_tokens", "user_id"], ascending=[False, True])


df = find_users_with_high_tokens(prompts)
df.head()
