from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Alice"],
    [2, "Bob"],
    [3, "Alex"],
    [4, "Donald"],
    [7, "Lee"],
    [13, "Jonathan"],
    [19, "Elvis"],
]
columns = ["id", "name"]
users = spark.createDataFrame(data, schema=columns)

data = [
    [1, 1, 120],
    [2, 2, 317],
    [3, 3, 222],
    [4, 7, 100],
    [5, 13, 312],
    [6, 19, 50],
    [7, 7, 120],
    [8, 19, 400],
    [9, 7, 230],
]
columns = ["id", "user_id", "distance"]
rides = spark.createDataFrame(data, schema=columns)

users = users.withColumnRenamed("id", "user_id")

df = (
    users.join(rides, on="user_id", how="left")
    .select("user_id", "name", "distance")
    .fillna(0)
)

df = (
    df.groupBy("user_id", "name")
    .agg({"distance": "sum"})
    .withColumnRenamed("sum(distance)", "travelled_distance")
    .select("name", "travelled_distance")
    .orderBy(["travelled_distance", "name"], ascending=[False, True])
)

df.show()
