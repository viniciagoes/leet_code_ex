from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 1, datetime.strptime("2019-07-20", "%Y-%m-%d"), "open_session"],
    [1, 1, datetime.strptime("2019-07-20", "%Y-%m-%d"), "scroll_down"],
    [1, 1, datetime.strptime("2019-07-20", "%Y-%m-%d"), "end_session"],
    [2, 4, datetime.strptime("2019-07-20", "%Y-%m-%d"), "open_session"],
    [2, 4, datetime.strptime("2019-07-21", "%Y-%m-%d"), "send_message"],
    [2, 4, datetime.strptime("2019-07-21", "%Y-%m-%d"), "end_session"],
    [3, 2, datetime.strptime("2019-07-21", "%Y-%m-%d"), "open_session"],
    [3, 2, datetime.strptime("2019-07-21", "%Y-%m-%d"), "send_message"],
    [3, 2, datetime.strptime("2019-07-21", "%Y-%m-%d"), "end_session"],
    [4, 3, datetime.strptime("2019-06-25", "%Y-%m-%d"), "open_session"],
    [4, 3, datetime.strptime("2019-06-25", "%Y-%m-%d"), "end_session"],
]
columns = (
    "user_id STRING, session_id STRING, activity_date TIMESTAMP, activity_type STRING"
)

activity = spark.createDataFrame(data=data, schema=columns)

df = activity.filter(
    (F.to_date(activity.activity_date) <= F.to_date(F.lit("2019-07-27")))
    & (
        F.to_date(activity.activity_date)
        > F.date_sub(F.to_date(F.lit("2019-07-27")), 30)
    )
)

df = df.groupBy("activity_date").agg(F.count_distinct("user_id"))

df = df.withColumnsRenamed(
    {"activity_date": "day", "count(DISTINCT user_id)": "active_users"}
)

df.show()
