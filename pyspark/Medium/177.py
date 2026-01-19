import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 100], [2, 200], [3, 300]]
columns = ["Id", "Salary"]
employee = spark.createDataFrame(data=data, schema=columns)
N = 2

df = (
    employee.select("Salary")
    .drop_duplicates()
    .orderBy("Salary", ascending=False)
    .offset(N - 1)
    .limit(1)
)
df.show()
