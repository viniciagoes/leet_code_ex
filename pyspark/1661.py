from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [0, 0, "start", 0.712],
    [0, 0, "end", 1.52],
    [0, 1, "start", 3.14],
    [0, 1, "end", 4.12],
    [1, 0, "start", 0.55],
    [1, 0, "end", 1.55],
    [1, 1, "start", 0.43],
    [1, 1, "end", 1.42],
    [2, 0, "start", 4.1],
    [2, 0, "end", 4.512],
    [2, 1, "start", 2.5],
    [2, 1, "end", 5.0],
]
schema = StructType(
    [
        StructField("machine_id", IntegerType(), True),
        StructField("process_id", IntegerType(), True),
        StructField("activity_type", StringType(), True),
        StructField("timestamp", DoubleType(), True),
    ]
)
activity = spark.createDataFrame(data, schema=schema)

df = (
    activity.filter(activity.activity_type == "start")
    .alias("a")
    .join(
        activity.filter(activity.activity_type == "end").alias("b"),
        on=["machine_id", "process_id"],
    )
)

df = df.withColumn("time", F.col("b.timestamp") - F.col("a.timestamp"))

df = df.groupBy("machine_id").agg({"time": "mean"})
df = df.withColumn("processing_time", F.round(F.col("avg(time)"), 3)).select(
    "machine_id", "processing_time"
)
df.show()
