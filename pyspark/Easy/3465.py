# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Widget A", "This is a sample product with SN1234-5678"],
    [2, "Widget B", "A product with serial SN9876-1234 in the description"],
    [3, "Widget C", "Product SN1234-56789 is available now"],
    [4, "Widget D", "No serial number here"],
    [5, "Widget E", "Check out SN4321-8765 in this description"],
]
columns = ["product_id", "product_name", "description"]
products = spark.createDataFrame(data=data, schema=columns)

df = products.filter(F.col("description").rlike(r"\bSN[0-9]{4}-[0-9]{4}\b"))

df.show()
