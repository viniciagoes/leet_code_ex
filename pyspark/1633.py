from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[6, "Alice"], [2, "Bob"], [7, "Alex"]]
columns = ["user_id", "user_name"]
users = spark.createDataFrame(data, schema=columns)

data = [
    [215, 6],
    [209, 2],
    [208, 2],
    [210, 6],
    [208, 6],
    [209, 7],
    [209, 6],
    [215, 7],
    [208, 7],
    [210, 2],
    [207, 2],
    [210, 7],
]
columns = ["contest_id", "user_id"]
register = spark.createDataFrame(data, schema=columns)

total = users.count()

df = register.groupBy("contest_id").count()

df = df.withColumn("percentage", F.round(F.col("count") * 100.0 / total, 2))

df = df.orderBy(F.desc("percentage"), F.asc("contest_id")).select(
    "contest_id", "percentage"
)
df.show()
