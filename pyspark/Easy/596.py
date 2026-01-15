from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["A", "Math"],
    ["B", "English"],
    ["C", "Math"],
    ["D", "Biology"],
    ["E", "Math"],
    ["F", "Computer"],
    ["G", "Math"],
    ["H", "Math"],
    ["I", "Math"],
]
courses = spark.createDataFrame(data, ["student", "class"])

df = courses.groupBy("class").count().alias("c")

df = df.filter(F.col("count") >= 5)

df.show()
