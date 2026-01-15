from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["1", "2020-11-28", "4", "32"],
    ["1", "2020-11-28", "55", "200"],
    ["1", "2020-12-3", "1", "42"],
    ["2", "2020-11-28", "3", "33"],
    ["2", "2020-12-9", "47", "74"],
]
columns = ["emp_id", "event_day", "in_time", "out_time"]
employees = spark.createDataFrame(data, schema=columns)

df = employees.withColumn("total_time", employees.out_time - employees.in_time)

df = df.groupBy(["event_day", "emp_id"]).agg(F.sum(df.total_time))

df = df.withColumnsRenamed({"event_day": "day", "sum(total_time)": "total_time"})

df.show()
