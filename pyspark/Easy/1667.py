from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, "aLice"], [2, "bOB"]]
columns = ["user_id", "name"]
users = spark.createDataFrame(data, schema=columns)

users = users.withColumn(
    "name",
    F.concat(
        F.upper(F.substring(F.col("name"), 1, 1)),
        F.lower(
            F.substring(
                F.col("name"), 2, 100
            )  # I tried F.length(F.col("name")) - 1 but it didnt work
        ),
    ),
)

users.show()
