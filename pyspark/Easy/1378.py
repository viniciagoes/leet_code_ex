from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, "Alice"], [7, "Bob"], [11, "Meir"], [90, "Winston"], [3, "Jonathan"]]
columns = ["id", "name"]
employees = spark.createDataFrame(data, schema=columns)

data = [[3, 1], [11, 2], [90, 3]]
columns = ["id", "unique_id"]
employee_uni = spark.createDataFrame(data, schema=columns)

df = employees.join(employee_uni, on="id", how="left").select("unique_id", "name")

df.show()
