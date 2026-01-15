from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [["0", "1"], ["1", "0"], ["2", "0"], ["2", "1"]]
columns = ["user_id", "follower_id"]
followers = spark.createDataFrame(data, schema=columns)

df = followers.groupBy("user_id").agg(F.count_distinct(followers.follower_id))

df = df.withColumnRenamed("count(DISTINCT follower_id)", "followers_count")
df = df.sort("user_id")
df.show()
