import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 2, 3], [1, 2, 4], [1, 3, 3], [2, 1, 1], [2, 2, 1], [2, 3, 1], [2, 4, 1]]
columns = ["teacher_id", "subject_id", "dept_id"]
teacher = spark.createDataFrame(data=data, schema=columns)

df = teacher.groupBy("teacher_id").agg(F.count_distinct("subject_id"))
df = df.withColumnRenamed("count(DISTINCT subject_id)", "cnt")

df.show()
