from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 2, "2016/06/03"],
    [1, 3, "2016/06/08"],
    [2, 3, "2016/06/08"],
    [3, 4, "2016/06/09"],
]
columns = ["requester_id", "accepter_id", "accept_date"]
request_accepted = spark.createDataFrame(data=data, schema=columns)

all_ids = (
    request_accepted.select("requester_id")
    .withColumnRenamed("requester_id", "id")
    .union(
        request_accepted.select("accepter_id").withColumnRenamed("accepter_id", "id")
    )
)

df = all_ids.groupBy("id").count()

df = df.orderBy("count", ascending=False).limit(1).withColumnRenamed("count", "num")
df.show()
