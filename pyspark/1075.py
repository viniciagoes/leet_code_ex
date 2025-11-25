from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 1], [1, 2], [1, 3], [2, 1], [2, 4], [2, 1]]
columns = ["project_id", "employee_id"]
project = spark.createDataFrame(data=data, schema=columns)

data = [[1, "Khaled", 3], [2, "Ali", 2], [3, "John", 1], [4, "Doe", 2]]
columns = ["employee_id", "name", "experience_years"]
employee = spark.createDataFrame(data=data, schema=columns)

df = project.join(employee, on="employee_id")

grouped_df = df.groupBy("project_id").agg({"experience_years": "avg"})

grouped_df = grouped_df.withColumn(
    "average_years", F.round(F.col("avg(experience_years)"), 2)
).select("project_id", "average_years")

grouped_df.show()
