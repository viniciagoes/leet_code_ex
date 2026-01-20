import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [1, 3.5], [2, 3.65], [3, 4.0], [4, 3.85], [5, 4.0], [6, 3.65]
columns = ["id", "score"]
scores = spark.createDataFrame(data=data, schema=columns)

df = scores.select("score").orderBy("score", ascending=False)

df = df.withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("score").desc())))
df.show()
