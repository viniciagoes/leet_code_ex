import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 100, 2008, 10, 5000], [2, 100, 2009, 12, 5000], [7, 200, 2011, 15, 9000]]
columns = ["sale_id", "product_id", "year", "quantity", "price"]
sales = spark.createDataFrame(data=data, schema=columns)

df = (
    sales.alias("s1")
    .join(
        sales.alias("s2")
        .groupBy("product_id")
        .agg(F.min(F.col("year")).alias("first_year")),
        (F.col("s1.product_id") == F.col("s2.product_id"))
        & (F.col("s1.year") == F.col("first_year")),
        how="inner",
    )
    .select("s1.product_id", "s1.year", "s1.quantity", "s1.price")
)

df.show()
