# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 300], [2, 300], [3, 300]]
columns = ["id", "salary"]
employee = spark.createDataFrame(data=data, schema=columns)

first_max = employee.agg(F.max(employee.salary)).collect()[0][0]

df = (
    employee.filter(employee.salary < first_max)
    .orderBy("salary", ascending=False)
    .select("salary")
    .withColumnRenamed("salary", "SecondHighestSalary")
    .limit(1)
)


if df.count() == 0:
    schema = StructType([StructField("SecondHighestSalary", LongType(), True)])
    df = spark.createDataFrame([(None,)], schema)

df.show()
