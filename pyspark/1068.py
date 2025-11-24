from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 100, 2008, 10, 5000], [2, 100, 2009, 12, 5000], [7, 200, 2011, 15, 9000]]
columns = ["sale_id", "product_id", "year", "quantity", "price"]
sales = spark.createDataFrame(data=data, schema=columns)

data = [[100, "Nokia"], [200, "Apple"], [300, "Samsung"]]
columns = ["product_id", "product_name"]
product = spark.createDataFrame(data=data, schema=columns)

df = sales.join(product, on="product_id").select("product_name", "year", "price")

df.show()
