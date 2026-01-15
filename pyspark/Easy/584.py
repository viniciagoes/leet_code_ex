from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [
    [1, "Will", None],
    [2, "Jane", None],
    [3, "Alex", 2],
    [4, "Bill", None],
    [5, "Zack", 1],
    [6, "Mark", 2],
]
customer = spark.createDataFrame(data, ["id", "name", "referee_id"])

df = customer.filter(
    ((customer.referee_id.isNull()) | (customer.referee_id != 2))
).select("name")

df.show()
