import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, "Abbot"], [2, "Doris"], [3, "Emerson"], [4, "Green"], [5, "Jeames"]]
columns = ["id", "student"]
seat = spark.createDataFrame(data=data, schema=columns)

df = seat.withColumn(
    "new_id", F.when(seat.id % 2 == 0, seat.id - 1).when(seat.id % 2 == 1, seat.id + 1)
)

df = (
    df.alias("df1")
    .join(df.alias("df2"), F.col("df1.new_id") == F.col("df2.id"), how="left")
    .select("df1.id", "df1.student", "df2.student")
    .orderBy("id")
)

df = (
    df.withColumn("new_student", F.coalesce(F.col("df2.student"), F.col("df1.student")))
    .select("id", "new_student")
    .withColumnRenamed("new_student", "student")
)

df.show()
