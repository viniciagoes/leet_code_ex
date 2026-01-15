from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 1], [2, 2], [3, 3], [4, 3]]
orders = spark.createDataFrame(data, ["order_number", "customer_number"])

# Agg count and group by customer_number
# Get max count
# Transform to int
df = orders.groupBy(F.col("customer_number")).agg(F.count("customer_number").alias("c"))
n = df.select(F.max(df.c))
max_count = n.collect()[0][0]

df = df.select("customer_number").filter(df.c == max_count)
df.show()
