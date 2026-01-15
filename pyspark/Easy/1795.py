from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[0, 95, 100, 105], [1, 70, None, 80]]
columns = ["product_id", "store1", "store2", "store3"]
products = spark.createDataFrame(data, schema=columns)

df = (
    products.melt(
        ids="product_id",
        values=["store1", "store2", "store3"],
        variableColumnName="store",
        valueColumnName="price",
    )
    .dropna(subset="price")
    .sort("product_id")
)

df.show()
