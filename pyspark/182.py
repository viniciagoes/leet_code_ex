from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [[1, 'a@b.com'], [2, 'c@d.com'], [3, 'a@b.com']]
df = spark.createDataFrame(data, ['id', 'email'])

df.groupBy("email") \
  .agg(F.count("email").alias("count")) \
  .filter(F.col("count") > 1) \
  .select("email") \
  .show()