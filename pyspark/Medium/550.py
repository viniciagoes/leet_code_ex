from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 2, "2016-03-01", 5],
    [1, 2, "2016-03-02", 6],
    [2, 3, "2017-06-25", 1],
    [3, 1, "2016-03-02", 0],
    [3, 4, "2018-07-03", 5],
]
columns = ["player_id", "device_id", "event_date", "games_played"]
data = [
    [player_id, device_id, datetime.strptime(event_date, "%Y-%m-%d"), games_played]
    for player_id, device_id, event_date, games_played in data
]
activity = spark.createDataFrame(data=data, schema=columns)

first_event = activity.groupBy("player_id").agg(
    F.min("event_date").alias("first_event_date")
)

activity_with_first = activity.join(first_event, on="player_id")

activity_next_day = activity_with_first.filter(
    F.expr("event_date = date_add(first_event_date, 1)")
)

numerator = activity_next_day.count()
denominator = activity.select("player_id").distinct().count()
fraction = round(numerator / denominator, 2) if denominator else 0.0

df = activity.sparkSession.createDataFrame([(fraction,)], ["fraction"])
