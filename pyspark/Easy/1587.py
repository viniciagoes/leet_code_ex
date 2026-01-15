from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[900001, "Alice"], [900002, "Bob"], [900003, "Charlie"]]
columns = ["account", "name"]
users = spark.createDataFrame(data, schema=columns)

data = [
    [1, 900001, 7000, "2020-08-01"],
    [2, 900001, 7000, "2020-09-01"],
    [3, 900001, -3000, "2020-09-02"],
    [4, 900002, 1000, "2020-09-12"],
    [5, 900003, 6000, "2020-08-07"],
    [6, 900003, 6000, "2020-09-07"],
    [7, 900003, -4000, "2020-09-11"],
]
columns = ["trans_id", "account", "amount", "transacted_on"]
transactions = spark.createDataFrame(data, schema=columns)

df = transactions.join(users, on="account", how="left")

df = df.groupBy("name").agg({"amount": "sum"})

df = df.filter(F.col("sum(amount)") > 10000).withColumnRenamed("sum(amount)", "balance")

df.show()
