from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "2018-01-01", "Lenovo"],
    [2, "2018-02-09", "Samsung"],
    [3, "2018-01-19", "LG"],
    [4, "2018-05-21", "HP"],
]
data = [
    [user_id, datetime.strptime(join_date, "%Y-%m-%d"), favorite_brand]
    for user_id, join_date, favorite_brand in data
]
columns = ["user_id", "join_date", "favorite_brand"]
users = spark.createDataFrame(data=data, schema=columns)

data = [
    [1, "2019-08-01", 4, 1, 2],
    [2, "2018-08-02", 2, 1, 3],
    [3, "2019-08-03", 3, 2, 3],
    [4, "2018-08-04", 1, 4, 2],
    [5, "2018-08-04", 1, 3, 4],
    [6, "2019-08-05", 2, 2, 4],
]
data = [
    [order_id, datetime.strptime(order_date, "%Y-%m-%d"), item_id, buyer_id, seller_id]
    for order_id, order_date, item_id, buyer_id, seller_id in data
]
columns = ["order_id", "order_date", "item_id", "buyer_id", "seller_id"]
orders = spark.createDataFrame(data=data, schema=columns)

data = [[1, "Samsung"], [2, "Lenovo"], [3, "LG"], [4, "HP"]]
columns = ["item_id", "item_brand"]
items = spark.createDataFrame(data=data, schema=columns)

orders_2019 = (
    orders.filter(F.year(orders.order_date) == 2019)
    .groupBy("buyer_id")
    .agg(F.count("order_id"))
)

df = (
    users.join(orders_2019, users.user_id == orders_2019.buyer_id, how="left")
    .select("user_id", "join_date", "count(order_id)")
    .fillna(0)
    .withColumnsRenamed({"user_id": "buyer_id", "count(order_id)": "orders_in_2019"})
)

df.show()
