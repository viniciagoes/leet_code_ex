from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [[8], [8], [7], [7], [3], [3], [6]]
columns = ["num"]
df = spark.createDataFrame(data=data, schema = columns)

df = df.groupBy("num").count()
df = df.filter(F.col("count") == 1)
df = df.agg({"num" : "max"})
df = df.withColumnRenamed("max(num)", "num")