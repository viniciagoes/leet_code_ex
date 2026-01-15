from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [9, "Hercy", None, 43],
    [6, "Alice", 9, 41],
    [4, "Bob", 9, 36],
    [2, "Winston", None, 37],
]
columns = ["employee_id", "name", "reports_to", "age"]
employees = spark.createDataFrame(data, schema=columns)

reports = (
    employees.dropna(subset="reports_to")
    .groupBy(F.col("reports_to"))
    .agg(F.count("employee_id"), F.round(F.mean("age")))
)

df = employees.select("employee_id", "name").join(
    reports, on=(employees.employee_id == reports.reports_to), how="inner"
)

df = df.withColumnsRenamed(
    {"count(employee_id)": "reports_count", "round(avg(age), 0)": "average_age"}
).select("employee_id", "name", "reports_count", "average_age")

df.show()
