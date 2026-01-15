from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [6, "2020-06-30 15:06:07"],
    [6, "2021-04-21 14:06:06"],
    [6, "2019-03-07 00:18:15"],
    [8, "2020-02-01 05:10:53"],
    [8, "2020-12-30 00:46:50"],
    [2, "2020-01-16 02:49:50"],
    [2, "2019-08-25 07:59:08"],
    [14, "2019-07-14 09:00:00"],
    [14, "2021-01-06 11:59:59"],
]

data = [[uid, datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")] for uid, ts in data]

columns = ["user_id", "time_stamp"]
logins = spark.createDataFrame(data=data, schema=columns)

df = logins.filter(F.year(logins.time_stamp) == 2020)

df = df.groupBy("user_id").agg(F.max(df.time_stamp))

df.show()
