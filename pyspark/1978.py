import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [3, "Mila", 9, 60301],
    [12, "Antonella", None, 31000],
    [13, "Emery", None, 67084],
    [1, "Kalel", 11, 21241],
    [9, "Mikaela", None, 50937],
    [11, "Joziah", 6, 28485],
]
columns = ["employee_id", "name", "manager_id", "salary"]
employees = spark.createDataFrame(data=data, schema=columns)

df = (
    employees.alias("e")
    .join(
        employees.alias("m"),
        F.col("e.manager_id") == F.col("m.employee_id"),
        how="left",
    )
    .select(
        F.col("e.employee_id"),
        F.col("e.salary"),
        F.col("e.manager_id"),
        F.col("m.employee_id").alias("manager_status"),
    )
)

df = (
    df.filter(
        (df.salary < 30000) & (df.manager_id.isNotNull()) & (df.manager_status.isNull())
    )
    .select("employee_id")
    .orderBy("employee_id")
)

df.show()
