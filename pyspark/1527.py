from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "Daniel", "YFEV COUGH"],
    [2, "Alice", ""],
    [3, "Bob", "DIAB100 MYOP"],
    [4, "George", "ACNE DIAB100"],
    [5, "Alain", "DIAB201"],
]
columns = ["patient_id", "patient_name", "conditions"]
patients = spark.createDataFrame(data, schema=columns)

df = patients.filter(
    patients.conditions.startswith("DIAB1") | patients.conditions.contains(" DIAB1")
)

df.show()
