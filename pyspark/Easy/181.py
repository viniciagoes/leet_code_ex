from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [
    [1, "Joe", 70000, 3],
    [2, "Henry", 80000, 4],
    [3, "Sam", 60000, None],
    [4, "Max", 90000, None],
]
employee = spark.createDataFrame(data, ["id", "name", "salary", "managerId"])

e = spark.createDataFrame(employee.rdd, schema=employee.schema).withColumnRenamed(
    "name", "Employee"
)
m = spark.createDataFrame(employee.rdd, schema=employee.schema).withColumnRenamed(
    "name", "Manager"
)

e.join(m, e.managerId == m.id, how="left").filter(e.salary > m.salary).select(
    "Employee"
)
