import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, None], [2, 1], [3, 1], [4, 2], [5, 2]]
schema = ["id", "p_id"]
tree = spark.createDataFrame(data=data, schema=schema)

df = (
    tree.alias("t1")
    .join(tree.alias("t2"), F.col("t1.id") == F.col("t2.p_id"), how="left")
    .select("t1.id", "t1.p_id", F.col("t2.id").alias("c_id"))
)

df = df.select(
    "id",
    F.when((df.p_id.isNull()) & (df.c_id.isNotNull()), "Root")
    .when((df.p_id.isNotNull()) & (df.c_id.isNotNull()), "Inner")
    .when((df.p_id.isNotNull()) & (df.c_id.isNull()), "Leaf")
    .otherwise("Root")
    .alias("type"),
)

df.show(10)
