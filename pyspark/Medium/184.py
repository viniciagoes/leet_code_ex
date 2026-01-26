import pyspark.sql.functions as F

# from datetime import datetime
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Joe", 70000, 1],
    [2, "Jim", 90000, 1],
    [3, "Henry", 80000, 2],
    [4, "Sam", 60000, 2],
    [5, "Max", 90000, 1],
]
columns = ["id", "name", "salary", "departmentId"]
employee = spark.createDataFrame(data=data, schema=columns)

data = [[1, "IT"], [2, "Sales"]]
columns = ["id", "name"]
department = spark.createDataFrame(data=data, schema=columns)

employee.show()
department.show()

df = employee.join(
    department.select(
        department.id.alias("departmentId"), department.name.alias("departmentName")
    ),
    on="departmentId",
)

w = Window().partitionBy("departmentId").orderBy(F.col("salary").desc())
df = df.withColumn("rank", F.dense_rank().over(w))

df = (
    df.filter(df.rank == 1)
    .select("departmentName", "name", "salary")
    .withColumnsRenamed(
        {"departmentName": "Department", "name": "Employee", "salary": "Salary"}
    )
)

df.show()
