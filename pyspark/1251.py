from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
import datetime
 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [
    [1, datetime.date(2019, 2, 17), datetime.date(2019, 2, 28), 5],
    [1, datetime.date(2019, 3, 1), datetime.date(2019, 3, 22), 20],
    [2, datetime.date(2019, 2, 1), datetime.date(2019, 2, 20), 15],
    [2, datetime.date(2019, 2, 21), datetime.date(2019, 3, 31), 30],
]
columns = ["product_id", "start_date", "end_date", "price"]
prices = spark.createDataFrame(data=data, schema=columns)

units_data = [
    [1, datetime.date(2019, 2, 25), 100],
    [1, datetime.date(2019, 3, 1), 15],
    [2, datetime.date(2019, 2, 10), 200],
    [2, datetime.date(2019, 3, 22), 30],
]
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("purchase_date", DateType(), True),
    StructField("units", IntegerType(), True),
])

units_sold = spark.createDataFrame(units_data, schema=schema)

df = prices.join(
    units_sold,
    on="product_id",
    how="left"
)

df = df.filter(
    (df.purchase_date.isNull()) | ((df.purchase_date >= df.start_date) & (df.purchase_date <= df.end_date))
)

df.fillna(value=0, subset=["price", "units"])
df = df.withColumn(
    "cost",
    df.price * df.units
)

df = df.groupBy(
    "product_id"
).agg(
    {
        "cost" : "sum",
        "units" : "sum"
    }
)

df = df.withColumn(
    "average_price",
    F.round(F.col("sum(cost)") / F.col("sum(units)"), 2)
)

df = df.fillna(0)
df = df.select(
    "product_id",
    "average_price"
)

df.show()