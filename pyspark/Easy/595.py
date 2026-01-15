from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    ["Afghanistan", "Asia", 652230, 25500100, 20343000000],
    ["Albania", "Europe", 28748, 2831741, 12960000000],
    ["Algeria", "Africa", 2381741, 37100000, 188681000000],
    ["Andorra", "Europe", 468, 78115, 3712000000],
    ["Angola", "Africa", 1246700, 20609294, 100990000000],
]
world = spark.createDataFrame(data, ["name", "continent", "area", "population", "gdp"])

df = world.filter((world.area >= 3000000) | (world.population >= 25000000)).select(
    ["name", "area", "population"]
)
df.show()
