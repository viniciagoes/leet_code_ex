import pandas as pd

data = [
    ["2020-05-30", "Headphone"],
    ["2020-06-01", "Pencil"],
    ["2020-06-02", "Mask"],
    ["2020-05-30", "Basketball"],
    ["2020-06-01", "Bible"],
    ["2020-06-02", "Mask"],
    ["2020-05-30", "T-Shirt"],
]
activities = pd.DataFrame(data, columns=["sell_date", "product"]).astype(
    {"sell_date": "datetime64[ns]", "product": "object"}
)


def categorize_products(activities: pd.DataFrame) -> pd.DataFrame:
    activities.sort_values(by=["sell_date", "product"], inplace=True)

    string_agg = (
        activities.groupby("sell_date")["product"]
        .unique()
        .apply(",".join)
        .reset_index()
    )

    distinct_count = activities.groupby("sell_date")["product"].nunique().reset_index()
    df = distinct_count.merge(string_agg, on="sell_date")
    df.rename(columns={"product_x": "num_sold", "product_y": "products"}, inplace=True)

    return df

    # Found this solution, way better and using less memory
    # return activities.groupby('sell_date')['product'].agg([
    #     ('num_sold', 'nunique'), # some aggs i think the documentation still lacks information
    #     ('products', lambda x: ','.join(sorted(x.unique()))) # same here
    # ]).reset_index()


df = categorize_products(activities)
print(df.head(30))
