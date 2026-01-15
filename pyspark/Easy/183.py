from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [[1, "Joe"], [2, "Henry"], [3, "Sam"], [4, "Max"]]
customers = spark.createDataFrame(data, ["id", "name"])
data = [[1, 3], [2, 1]]
orders = spark.createDataFrame(data, ["id", "customerId"])

df = customers.join(orders, customers.id == orders.customerId, how="leftanti")

df.show()
