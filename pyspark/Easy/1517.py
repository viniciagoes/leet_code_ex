from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Winston", "winston@leetcode.com"],
    [2, "Jonathan", "jonathanisgreat"],
    [3, "Annabelle", "bella-@leetcode.com"],
    [4, "Sally", "sally.come@leetcode.com"],
    [5, "Marwan", "quarz#2020@leetcode.com"],
    [6, "David", "david69@gmail.com"],
    [7, "Shapiro", ".shapo@leetcode.com"],
]
columns = ["user_id", "name", "mail"]
users = spark.createDataFrame(data, schema=columns)

df = users.filter(F.col("mail").rlike(r"^[a-zA-Z][a-zA-Z0-9_.-]*@leetcode\.com$"))
df.show()
