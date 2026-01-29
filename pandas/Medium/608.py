import pandas as pd

data = [[1, None], [2, 1], [3, 1], [4, 2], [5, 2]]
tree = pd.DataFrame(data, columns=["id", "p_id"]).astype(
    {"id": "Int64", "p_id": "Int64"}
)


def tree_node(tree: pd.DataFrame) -> pd.DataFrame:
    df = tree.merge(
        tree.rename(columns={"id": "c_id"}),
        left_on="id",
        right_on="p_id",
        how="left",
    )
    df = df.loc[:, ["id", "p_id_x", "c_id"]]

    df["type"] = "Root"
    df.loc[(~df["p_id_x"].isna()) & (~df["c_id"].isna()), "type"] = "Inner"
    df.loc[(~df["p_id_x"].isna()) & (df["c_id"].isna()), "type"] = "Leaf"

    return df.loc[:, ["id", "type"]].drop_duplicates()
