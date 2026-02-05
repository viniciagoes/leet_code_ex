import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 5], [2, 6], [3, 5], [3, 6], [1, 6]]
columns = ["customer_id", "product_key"]
customer = spark.createDataFrame(data=data, schema=columns)

data = [[5], [6]]
columns = ["product_key"]
product = spark.createDataFrame(data=data, schema=columns)

products = product.agg(F.count_distinct("product_key").alias("all"))
products = products.first()["all"]

df = customer.groupBy("customer_id").agg(
    F.count_distinct("product_key").alias("distinct_products")
)

df = df.filter(F.col("distinct_products") == F.lit(products))

df.show()
