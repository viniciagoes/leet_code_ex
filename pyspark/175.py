from pyspark.sql import DataFrame, SparkSession
from typing import List
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark= SparkSession \
       .builder \
       .appName("Our First Spark Example") \
       .getOrCreate()


data = [[1, 'Wang', 'Allen'], [2, 'Alice', 'Bob']]
person = spark.createDataFrame(data, ['personId', 'firstName', 'lastName'])

data = [[1, 2, 'New York City', 'New York'], [2, 3, 'Leetcode', 'California']]
address = spark.createDataFrame(data, ['addressId', 'personId', 'city', 'state'])

def combine_two_tables(person: DataFrame, address: DataFrame) -> DataFrame:
    df = person.join(
      address,
      on="personId",
      how="left"
    ).select(["firstName", "lastName", "city", "state"])

    return df