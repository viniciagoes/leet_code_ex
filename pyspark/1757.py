from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["0", "Y", "N"],
    ["1", "Y", "Y"],
    ["2", "N", "Y"],
    ["3", "Y", "Y"],
    ["4", "N", "N"],
]
columns = ["product_id", "low_fats", "recyclable"]
products = spark.createDataFrame(data, schema=columns)

df = products.filter((products.low_fats == "Y") & (products.recyclable == "Y")).select(
    "product_id"
)

df.show()
