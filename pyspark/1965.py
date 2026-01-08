from pyspark.sql import SparkSession
# from datetime import datetime
# import pyspark.sql.functions as F

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [[2, 'Crew'], [4, 'Haven'], [5, 'Kristian']]
columns=['employee_id', 'name']
employees = spark.createDataFrame(data=data, schema = columns)

data = [[5, 76071], [1, 22517], [4, 63539]]
columns=['employee_id', 'salary']
salaries = spark.createDataFrame(data=data, schema = columns)

df = employees.join(
    salaries,
    on="employee_id",
    how="full"
)

df = df.filter(
    df.name.isNull() | df.salary.isNull()
).select(
    "employee_id"
).orderBy(
    "employee_id"
)

df.show()
