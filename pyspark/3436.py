import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "alice@example.com"],
    [2, "bob_at_example.com"],
    [3, "charlie@example.net"],
    [4, "david@domain.com"],
    [5, "eve@invalid"],
]
columns = ["user_id", "email"]
users = spark.createDataFrame(data=data, schema=columns)

df = users.filter(F.col("email").rlike(r"^[a-zA-Z0-9_]+@[a-zA-Z]+\.com$"))

df.show()
