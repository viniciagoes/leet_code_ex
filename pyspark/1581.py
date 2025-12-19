from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 23], [2, 9], [4, 30], [5, 54], [6, 96], [7, 54], [8, 54]]
columns = ["visit_id", "customer_id"]
visits = spark.createDataFrame(data, schema=columns)

data = [[2, 5, 310], [3, 5, 300], [9, 5, 200], [12, 1, 910], [13, 2, 970]]
columns = ["transaction_id", "visit_id", "amount"]
transactions = spark.createDataFrame(data, schema=columns)

df = visits.join(transactions, on="visit_id", how="left")

df = (
    df.filter(df.transaction_id.isNull())
    .groupBy("customer_id")
    .count()
    .withColumnRenamed("count", "count_no_trans")
)

df.show()
