from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Jhon", "2019-01-01", 100],
    [2, "Daniel", "2019-01-02", 110],
    [3, "Jade", "2019-01-03", 120],
    [4, "Khaled", "2019-01-04", 130],
    [5, "Winston", "2019-01-05", 110],
    [6, "Elvis", "2019-01-06", 140],
    [7, "Anna", "2019-01-07", 150],
    [8, "Maria", "2019-01-08", 80],
    [9, "Jaze", "2019-01-09", 110],
    [1, "Jhon", "2019-01-10", 130],
    [3, "Jade", "2019-01-10", 150],
]
data = [
    [customer_id, name, datetime.strptime(visited_on, "%Y-%m-%d"), amount]
    for customer_id, name, visited_on, amount in data
]
columns = ["customer_id", "name", "visited_on", "amount"]
customer = spark.createDataFrame(data=data, schema=columns)


def restaurant_growth(customer):
    # sum amounts per day
    grouped = customer.groupBy("visited_on").agg(F.sum("amount").alias("amount"))
    # 7-day rolling (current + 6 preceding)
    w = Window.orderBy("visited_on").rowsBetween(-6, 0)
    df = (
        grouped.withColumn("amount", F.sum("amount").over(w))
        .withColumn("window_count", F.count("amount").over(w))
        .filter(F.col("window_count") >= 7)  # keep only days with 6 preceding days
        .withColumn("average_amount", F.round(F.col("amount") / F.lit(7.0), 2))
        .select(
            F.col("visited_on"),
            F.round(F.col("amount").cast("double"), 2).alias("amount"),
            F.col("average_amount"),
        )
    )
    return df


df = restaurant_growth(customer)
df.show()
