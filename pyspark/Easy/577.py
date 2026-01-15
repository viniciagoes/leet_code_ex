from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [
    [3, "Brad", None, 4000],
    [1, "John", 3, 1000],
    [2, "Dan", 3, 2000],
    [4, "Thomas", 3, 4000],
]
employee = spark.createDataFrame(data, ["empId", "name", "supervisor", "salary"])
data = [[2, 500], [4, 2000]]
bonus = spark.createDataFrame(data, ["empId", "bonus"])

df = employee.join(bonus, on="empId", how="left")

df = df.filter((F.col("bonus").isNull()) | (F.col("bonus") < 1000)).select(
    ["name", "bonus"]
)

df.show()
