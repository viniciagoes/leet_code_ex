from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initial setup
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

# Data setup
data = [[13, 15, 30], [10, 20, 15]]
columns = ["x", "y", "z"]
triangles = spark.createDataFrame(data=data, schema=columns)

df = triangles.withColumn(
    "Triangle",
    F.when(
        (
            (triangles.x + triangles.y > triangles.z)
            & (triangles.x + triangles.z > triangles.y)
            & (triangles.y + triangles.z > triangles.x)
        ),
        "Yes",
    ).otherwise("No"),
)

df.show()
