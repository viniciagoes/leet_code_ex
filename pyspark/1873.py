from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [2, "Meir", 3000],
    [3, "Michael", 3800],
    [7, "Addilyn", 7400],
    [8, "Juan", 6100],
    [9, "Kannon", 7700],
]
columns = ["employee_id", "name", "salary"]
employees = spark.createDataFrame(data, schema=columns)

df = employees.withColumn(
    "bonus",
    F.when(
        (employees.employee_id % 2 == 1) & ~(employees.name.startswith("M")),
        employees.salary,
    ).otherwise(0),
).select("employee_id", "bonus")

df.show()
