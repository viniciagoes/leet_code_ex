from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [[1, "S8", 1000], [2, "G4", 800], [3, "iPhone", 1400]]
columns = ["product_id", "product_name", "unit_price"]
products = spark.createDataFrame(data=data, schema=columns)

sales_data = [
    [1, 1, 1, datetime.strptime("2019-01-21", "%Y-%m-%d"), 2, 2000],
    [1, 2, 2, datetime.strptime("2019-02-17", "%Y-%m-%d"), 1, 800],
    [2, 2, 3, datetime.strptime("2019-06-02", "%Y-%m-%d"), 1, 800],
    [3, 3, 4, datetime.strptime("2019-05-13", "%Y-%m-%d"), 2, 2800],
]
sales_schema = "seller_id LONG, product_id LONG, buyer_id LONG, sale_date TIMESTAMP, quantity LONG, price LONG"
sales = spark.createDataFrame(data=sales_data, schema=sales_schema)

invalid_items = (
    sales.filter(
        (sales.sale_date < datetime.strptime("2019-01-01", "%Y-%m-%d"))
        | (sales.sale_date > datetime.strptime("2019-03-31", "%Y-%m-%d"))
    )
    .select("product_id")
    .distinct()
)

invalid_items_list = [int(row.product_id) for row in invalid_items.collect()]

df = (
    sales.join(products, on="product_id")
    .filter((~(F.col("product_id").isin(invalid_items_list))))
    .select("product_id", "product_name")
    .distinct()
)
