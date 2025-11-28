from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, 3, 5, datetime.strptime("2019-08-01", "%Y-%m-%d")],
    [1, 3, 6, datetime.strptime("2019-08-02", "%Y-%m-%d")],
    [2, 7, 7, datetime.strptime("2019-08-01", "%Y-%m-%d")],
    [2, 7, 6, datetime.strptime("2019-08-02", "%Y-%m-%d")],
    [4, 7, 1, datetime.strptime("2019-07-22", "%Y-%m-%d")],
    [3, 4, 4, datetime.strptime("2019-07-21", "%Y-%m-%d")],
    [3, 4, 4, datetime.strptime("2019-07-21", "%Y-%m-%d")],
]
columns = "article_id INT, author_id INT, viewer_id INT, view_date TIMESTAMP"

views = spark.createDataFrame(data=data, schema=columns)

df = (
    views.filter((views.author_id == views.viewer_id))
    .select("author_id")
    .withColumnRenamed("author_id", "id")
    .drop_duplicates()
    .orderBy("id", ascending=True)
)

df.show()
