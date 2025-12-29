import pandas as pd

data = [["0", "1"], ["1", "0"], ["2", "0"], ["2", "1"]]
followers = pd.DataFrame(data, columns=["user_id", "follower_id"]).astype(
    {"user_id": "Int64", "follower_id": "Int64"}
)


def count_followers(followers: pd.DataFrame) -> pd.DataFrame:
    df = followers.groupby("user_id")["follower_id"].nunique().reset_index()

    df.rename(columns={"follower_id": "followers_count"}, inplace=True)
    df.sort_values("user_id", inplace=True)
    return df


df = count_followers(followers)
df.head()
