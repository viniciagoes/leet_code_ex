from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 8000, "Jan"],
    [2, 9000, "Jan"],
    [3, 10000, "Feb"],
    [1, 7000, "Feb"],
    [1, 6000, "Mar"],
]
columns = ["id", "revenue", "month"]

department = spark.createDataFrame(data=data, schema=columns)

df = department.groupBy("id").pivot("month").sum("revenue")

months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]
df = df.select(
    "id",
    *[
        F.col(month) if month in df.columns else F.lit(None).cast("bigint").alias(month)
        for month in months
    ],
)

rename = {col: f"{col}_Revenue" for col in df.columns}
df = df.withColumnsRenamed(rename)

df.show()
