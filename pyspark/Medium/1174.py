from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()


data = [
    [1, 1, "2019-08-01", "2019-08-02"],
    [2, 2, "2019-08-02", "2019-08-02"],
    [3, 1, "2019-08-11", "2019-08-12"],
    [4, 3, "2019-08-24", "2019-08-24"],
    [5, 3, "2019-08-21", "2019-08-22"],
    [6, 2, "2019-08-11", "2019-08-13"],
    [7, 4, "2019-08-09", "2019-08-09"],
]
data = [
    [
        delivery_id,
        customer_id,
        datetime.strptime(order_date, "%Y-%m-%d"),
        datetime.strptime(customer_pref_delivery_date, "%Y-%m-%d"),
    ]
    for delivery_id, customer_id, order_date, customer_pref_delivery_date in data
]

delivery = spark.createDataFrame(
    data=data,
    schema=["delivery_id", "customer_id", "order_date", "customer_pref_delivery_date"],
)

df = (
    delivery.alias("d1")
    .join(
        delivery.alias("d2")
        .groupBy("d2.customer_id")
        .agg(F.min(F.col("d2.order_date")).alias("min_order_date")),
        (F.col("d1.customer_id") == F.col("d2.customer_id"))
        & (F.col("d1.order_date") == F.col("min_order_date")),
        how="inner",
    )
    .select("d1.customer_id", "order_date", "customer_pref_delivery_date")
)

df = df.withColumn(
    "immediate",
    F.when(
        F.col("order_date") == F.col("customer_pref_delivery_date"), F.lit(1)
    ).otherwise(F.lit(0)),
)

df = (
    df.groupBy()
    .agg(F.round(F.sum(F.col("immediate")) / F.count("*") * 100, 2))
    .withColumnRenamed(
        "round(((sum(immediate) / count(1)) * 100), 2)", "immediate_percentage"
    )
)

df.show()
