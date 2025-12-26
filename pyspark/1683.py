from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, "Let us Code"], [2, "More than fifteen chars are here!"]]
columns = ["tweet_id", "content"]
tweets = spark.createDataFrame(data, schema=columns)

df = tweets.filter(F.length(tweets.content) > 15).select("tweet_id")
df.show()
