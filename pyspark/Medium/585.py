import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
data = [
    [1, 10.0, 5.0, 10.0, 10.0],
    [2, 20.0, 20.0, 20.0, 20.0],
    [3, 10.0, 30.0, 20.0, 20.0],
    [4, 10.0, 40.0, 40.0, 40.0],
]
columns = ["pid", "tiv_2015", "tiv_2016", "lat", "lon"]
insurance = spark.createDataFrame(data=data, schema=columns)

crit_1 = insurance.groupBy(["lat", "lon"]).count()
crit_1 = crit_1.filter(F.col("count") == 1)

crit_2 = insurance.groupBy("tiv_2015").count()
crit_2 = crit_2.filter(F.col("count") > 1)

df = (
    insurance.join(crit_1, on=["lat", "lon"], how="inner")
    .join(crit_2, on="tiv_2015", how="inner")
    .agg({"tiv_2016": "sum"})
    .withColumnRenamed("sum(tiv_2016)", "tiv_2016")
)

df.show()
