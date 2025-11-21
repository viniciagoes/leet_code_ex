from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "A", "m", 2500],
    [2, "B", "f", 1500],
    [3, "C", "m", 5500],
    [4, "D", "f", 500],
]
columns = ["id", "name", "sex", "salary"]

df = spark.createDataFrame(data=data, schema=columns)

df = df.withColumn("sex", F.col("sex")).replace({"m": "f", "f": "m"})

df.show()
