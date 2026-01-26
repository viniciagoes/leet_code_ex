import pyspark.sql.functions as F

# from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [101, "John", "A", None],
    [102, "Dan", "A", 101],
    [103, "James", "A", 101],
    [104, "Amy", "A", 101],
    [105, "Anne", "A", 101],
    [106, "Ron", "B", 101],
]
columns = ["id", "name", "department", "managerId"]
employee = spark.createDataFrame(data=data, schema=columns)

top_managers = employee.groupBy("managerId").agg(F.count(F.col("managerId"))).dropna()
top_managers = top_managers.filter(F.col("count(managerId)") >= 5).select("managerId")

manager_ids = [row["managerId"] for row in top_managers.collect()]
df = employee.filter(F.col("id").isin(manager_ids)).select("name")
df.show()
