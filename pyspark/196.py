from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("test") \
      .getOrCreate()

data = [[1, 'john@example.com'], [2, 'bob@example.com'], [3, 'john@example.com']]
person = spark.createDataFrame(data, ['id', 'email'])

df = person.sort(person.id.asc()).dropDuplicates(subset=["email"])

df.show()