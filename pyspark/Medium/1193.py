from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [121, "US", "approved", 1000, "2018-12-18"],
    [122, "US", "declined", 2000, "2018-12-19"],
    [123, "US", "approved", 2000, "2019-01-01"],
    [124, "DE", "approved", 2000, "2019-01-07"],
    [125, None, "declined", 2000, "2019-01-07"],
]
data = [
    [id, country, state, amount, datetime.strptime(trans_date, "%Y-%m-%d")]
    for id, country, state, amount, trans_date in data
]
columns = ["id", "country", "state", "amount", "trans_date"]

transactions = spark.createDataFrame(data=data, schema=columns)

transactions = transactions.withColumn(
    "month", F.date_format(F.col("trans_date"), "yyyy-MM")
)

approved = (
    transactions.filter(transactions.state == "approved")
    .groupBy(["country", "month"])
    .agg(
        F.count("id").alias("approved_count"),
        F.sum("amount").alias("approved_total_amount"),
    )
)

all = transactions.groupby(["country", "month"]).agg(
    F.count("id").alias("trans_count"), F.sum("amount").alias("trans_total_amount")
)


df = (
    all.alias("a")
    .join(
        approved.alias("ap"),
        (all.country == approved.country) & (all.month == approved.month),
        how="left",
    )
    .select(
        "a.country",
        "a.month",
        "approved_count",
        "trans_count",
        "approved_total_amount",
        "trans_total_amount",
    )
)

df = df.withColumns(
    {
        "approved_count": F.coalesce(F.col("approved_count"), F.lit(0)),
        "approved_total_amount": F.coalesce(F.col("approved_total_amount"), F.lit(0)),
        "trans_count": F.coalesce(F.col("trans_count"), F.lit(0)),
        "trans_total_amount": F.coalesce(F.col("trans_total_amount"), F.lit(0)),
    }
)

df.show()
