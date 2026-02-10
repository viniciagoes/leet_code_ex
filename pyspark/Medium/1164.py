from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 20, "2019-08-14"],
    [2, 50, "2019-08-14"],
    [1, 30, "2019-08-15"],
    [1, 35, "2019-08-16"],
    [2, 65, "2019-08-17"],
    [3, 20, "2019-08-18"],
]
data = [
    [product_id, new_price, datetime.strptime(change_date, "%Y-%m-%d")]
    for product_id, new_price, change_date in data
]
columns = ["product_id", "new_price", "change_date"]
products = spark.createDataFrame(data=data, schema=columns)

std_values = products.groupBy("product_id").agg(F.min("change_date"))
std_values = std_values.filter(F.col("min(change_date)") > datetime(2019, 8, 16))
std_values = std_values.withColumn("price", F.lit(10)).select("product_id", "price")

valid_values = (
    products.filter(F.col("change_date") <= datetime(2019, 8, 16))
    .groupBy("product_id")
    .agg(F.max("change_date"))
    .withColumnRenamed("product_id", "product_id_vv")
)

valid_values = (
    products.join(
        valid_values,
        (F.col("change_date") == F.col("max(change_date)"))
        & (products.product_id == valid_values.product_id_vv),
    )
    .select("product_id", "new_price")
    .withColumnRenamed("new_price", "price")
)

df = valid_values.union(std_values)
df.show()
