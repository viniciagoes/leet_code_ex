import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Leetcode Solutions", "Book"],
    [2, "Jewels of Stringology", "Book"],
    [3, "HP", "Laptop"],
    [4, "Lenovo", "Laptop"],
    [5, "Leetcode Kit", "T-shirt"],
]
columns = ["product_id", "product_name", "product_category"]
products = spark.createDataFrame(data, schema=columns)

data = [
    [1, datetime.date.fromisoformat("2020-02-05"), 60],
    [1, datetime.date.fromisoformat("2020-02-10"), 70],
    [2, datetime.date.fromisoformat("2020-01-18"), 30],
    [2, datetime.date.fromisoformat("2020-02-11"), 80],
    [3, datetime.date.fromisoformat("2020-02-17"), 2],
    [3, datetime.date.fromisoformat("2020-02-24"), 3],
    [4, datetime.date.fromisoformat("2020-03-01"), 20],
    [4, datetime.date.fromisoformat("2020-03-04"), 30],
    [4, datetime.date.fromisoformat("2020-03-04"), 60],
    [5, datetime.date.fromisoformat("2020-02-25"), 50],
    [5, datetime.date.fromisoformat("2020-02-27"), 50],
    [5, datetime.date.fromisoformat("2020-03-01"), 50],
]
columns = ["product_id", "order_date", "unit"]
orders = spark.createDataFrame(data, schema=columns)

orders = orders.filter(
    (F.year(orders.order_date) == 2020) & (F.month(orders.order_date) == 2)
)

df = products.join(orders, on="product_id", how="inner")

df = (
    df.groupBy("product_name")
    .agg({"unit": "sum"})
    .withColumnRenamed("sum(unit)", "unit")
)

df = df.filter(df.unit >= 100)

df.show()
