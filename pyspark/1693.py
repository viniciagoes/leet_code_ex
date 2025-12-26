from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["2020-12-8", "toyota", 0, 1],
    ["2020-12-8", "toyota", 1, 0],
    ["2020-12-8", "toyota", 1, 2],
    ["2020-12-7", "toyota", 0, 2],
    ["2020-12-7", "toyota", 0, 1],
    ["2020-12-8", "honda", 1, 2],
    ["2020-12-8", "honda", 2, 1],
    ["2020-12-7", "honda", 0, 1],
    ["2020-12-7", "honda", 1, 2],
    ["2020-12-7", "honda", 2, 1],
]
columns = ["date_id", "make_name", "lead_id", "partner_id"]
daily_sales = spark.createDataFrame(data, schema=columns)

df = daily_sales.groupBy("date_id", "make_name").agg(
    F.count_distinct("lead_id").alias("unique_leads"),
    F.count_distinct("partner_id").alias("unique_partners"),
)

df.show()
