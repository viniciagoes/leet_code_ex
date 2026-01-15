from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["Dog", "Golden Retriever", 1, 5],
    ["Dog", "German Shepherd", 2, 5],
    ["Dog", "Mule", 200, 1],
    ["Cat", "Shirazi", 5, 2],
    ["Cat", "Siamese", 3, 3],
    ["Cat", "Sphynx", 7, 4],
]
columns = ["query_name", "result", "position", "rating"]

# sales_schema = "seller_id LONG, product_id LONG, buyer_id LONG, sale_date TIMESTAMP, quantity LONG, price LONG"
queries = spark.createDataFrame(data=data, schema=columns)

df = queries.withColumn("quality", queries.rating / queries.position + 1e-10)

w = Window.partitionBy("query_name")

df = (
    df.withColumn("is_below_3", F.when(F.col("rating") < 3, 1).otherwise(0))
    .withColumn(
        "poor_query_percentage",
        F.round((F.sum("is_below_3").over(w) / F.count("*").over(w)) * 100, 2),
    )
    .drop("is_below_3")
)

df = df.select("query_name", "quality", "poor_query_percentage")
