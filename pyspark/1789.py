from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["1", "1", "N"],
    ["2", "1", "Y"],
    ["2", "2", "N"],
    ["3", "3", "N"],
    ["4", "2", "N"],
    ["4", "3", "Y"],
    ["4", "4", "N"],
]
columns = ["employee_id", "department_id", "primary_flag"]
employee = spark.createDataFrame(data, schema=columns)

changed = employee.groupBy("employee_id").count()
changed = changed.collect()
changed = [row["employee_id"] for row in changed if row["count"] > 1]

df = employee.filter(
    ((employee.employee_id.isin(changed)) & (employee.primary_flag == "Y"))
    | (~(employee.employee_id.isin(changed)) & (employee.primary_flag == "N"))
).select("employee_id", "department_id")

df.show()
