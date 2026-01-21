import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, 1], [2, 1], [3, 1], [4, 2], [5, 1], [6, 2], [7, 2]]
columns = ["id", "num"]
logs = spark.createDataFrame(data=data, schema=columns)

w = Window().orderBy("id")
df = logs.withColumns(
    {
        "next_num": F.lead("num").over(w),
        "next_id": F.lead("id").over(w),
        "last_num": F.lag("num").over(w),
        "last_id": F.lag("id").over(w),
    }
)

df = (
    df.filter(
        (df.num == df.last_num)
        & (df.num == df.next_num)
        & (df.id == df.last_id + 1)
        & (df.id == df.next_id - 1)
    )
    .select("num")
    .drop_duplicates()
)

df.show()
