from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [[1, 'War', 'great 3D', 8.9], [2, 'Science', 'fiction', 8.5], [3, 'irish', 'boring', 6.2], [4, 'Ice song', 'Fantacy', 8.6], [5, 'House card', 'Interesting', 9.1]]
columns = ['id', 'movie', 'description', 'rating']

df = spark.createDataFrame(data=data, schema = columns)

df = df.filter(
    ((df.id % 2 != 0) & (df.description != "boring"))
).sort(
    F.desc("rating")
)

df.show()