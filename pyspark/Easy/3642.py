# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "The Great Gatsby", "F. Scott", "Fiction", 180],
    [2, "To Kill a Mockingbird", "Harper Lee", "Fiction", 281],
    [3, "1984", "George Orwell", "Dystopian", 328],
    [4, "Pride and Prejudice", "Jane Austen", "Romance", 432],
    [5, "The Catcher in the Rye", "J.D. Salinger", "Fiction", 277],
]
columns = ["book_id", "title", "author", "genre", "pages"]
books = spark.createDataFrame(data=data, schema=columns)

data = [
    [1, 1, "Alice", 50, 5],
    [2, 1, "Bob", 60, 1],
    [3, 1, "Carol", 40, 4],
    [4, 1, "David", 30, 2],
    [5, 1, "Emma", 45, 5],
    [6, 2, "Frank", 80, 4],
    [7, 2, "Grace", 70, 4],
    [8, 2, "Henry", 90, 5],
    [9, 2, "Ivy", 60, 4],
    [10, 2, "Jack", 75, 4],
    [11, 3, "Kate", 100, 2],
    [12, 3, "Liam", 120, 1],
    [13, 3, "Mia", 80, 2],
    [14, 3, "Noah", 90, 1],
    [15, 3, "Olivia", 110, 4],
    [16, 3, "Paul", 95, 5],
    [17, 4, "Quinn", 150, 3],
    [18, 4, "Ruby", 140, 3],
    [19, 5, "Sam", 80, 1],
    [20, 5, "Tara", 70, 2],
]
columns = ["session_id", "book_id", "reader_name", "pages_read", "session_rating"]
reading_sessions = spark.createDataFrame(data=data, schema=columns)

round2 = lambda x: round(x + 0.00001, 2)

# Determine the necessary counts and max/min per book_id
df = reading_sessions.groupBy("book_id").agg(
    F.sum(F.when(F.col("session_rating") == 3, 1).otherwise(0)).alias("three_count"),
    F.count("session_rating").alias("total_count"),
    F.max("session_rating").alias("max_score"),
    F.min("session_rating").alias("min_score"),
)

df = df.withColumn("rating_spread", df.max_score - df.min_score)

df = df.withColumn(
    "polarization_score", (df.total_count - df.three_count) / df.total_count
)

df = df.filter(
    (df.min_score < 3)
    & (df.max_score > 3)
    & (df.total_count >= 5)
    & (df.polarization_score >= 0.6)
)


final_df = (
    df.join(books, on="book_id")
    .orderBy(["polarization_score", "title"], ascending=[False, True])
    .select(
        "book_id",
        "title",
        "author",
        "genre",
        "pages",
        "rating_spread",
        "polarization_score",
    )
)

final_df.show()
