from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 1, 0], [1, 1, 1], [1, 1, 2], [1, 2, 3], [1, 2, 4], [2, 1, 5], [2, 1, 6]]
columns = ["actor_id", "director_id", "timestamp"]

df = spark.createDataFrame(data=data, schema=columns)
df = df.groupBy(["actor_id", "director_id"]).count()
df = df.filter(F.col("count") >= 3).select(["actor_id", "director_id"])

df.show()
