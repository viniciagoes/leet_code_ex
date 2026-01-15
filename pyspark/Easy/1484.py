from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()    

data = [
    [datetime.date.fromisoformat("2020-05-30"), "Headphone"],
    [datetime.date.fromisoformat("2020-06-01"), "Pencil"],
    [datetime.date.fromisoformat("2020-06-02"), "Mask"],
    [datetime.date.fromisoformat("2020-05-30"), "Basketball"],
    [datetime.date.fromisoformat("2020-06-01"), "Bible"],
    [datetime.date.fromisoformat("2020-06-02"), "Mask"],
    [datetime.date.fromisoformat("2020-05-30"), "T-Shirt"],
]
columns=["sell_date", "product"]
activities = spark.createDataFrame(data, schema=columns)

df = activities.drop_duplicates().orderBy(["sell_date", "product"])

df = df.groupBy("sell_date").agg(
    F.count("product").alias("num_sold"),
    F.concat_ws(",", F.collect_list("product")).alias("products")
)
