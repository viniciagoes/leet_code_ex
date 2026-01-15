from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [[1, None], [2, "Bob"], [13, "John"], [6, "Alex"]]
columns = ["student_id", "student_name"]
students = spark.createDataFrame(data, schema=columns)

data = [["Math"], ["Physics"], ["Programming"]]
columns = ["subject_name"]
subjects = spark.createDataFrame(data, schema=columns)

data = [
    [1, "Math"],
    [1, "Physics"],
    [1, "Programming"],
    [2, "Programming"],
    [1, "Physics"],
    [1, "Math"],
    [13, "Math"],
    [13, "Programming"],
    [13, "Physics"],
    [2, "Math"],
    [1, "Math"],
]
columns = ["student_id", "subject_name"]
examinations = spark.createDataFrame(data, schema=columns)

stu_vs_sub = students.join(subjects, how="cross")

exams = examinations.groupBy(["student_id", "subject_name"]).count()

df = stu_vs_sub.join(exams, on=["student_id", "subject_name"], how="left")
df = df.fillna(0).sort(["student_id", "subject_name"])

df.show()
