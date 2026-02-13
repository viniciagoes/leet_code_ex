import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [5, "Alice", 250, 1],
    [4, "Bob", 175, 5],
    [3, "Alex", 350, 2],
    [6, "John Cena", 400, 3],
    [1, "Winston", 500, 6],
    [2, "Marie", 200, 4],
]
columns = ["person_id", "person_name", "weight", "turn"]
queue = spark.createDataFrame(data=data, schema=columns)

w = Window.orderBy("turn")

df = queue.groupBy(["person_name", "weight", "turn"]).agg(
    F.sum("weight").over(w).alias("total_weight")
)

df = (
    df.filter(df.total_weight <= 1000)
    .orderBy("total_weight", ascending=False)
    .select("person_name")
    .limit(1)
)

df.show()
