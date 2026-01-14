# from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "The Great Gatsby", "F. Scott", "Fiction", 1925, 3],
    [2, "To Kill a Mockingbird", "Harper Lee", "Fiction", 1960, 3],
    [3, "1984", "George Orwell", "Dystopian", 1949, 1],
    [4, "Pride and Prejudice", "Jane Austen", "Romance", 1813, 2],
    [5, "The Catcher in the Rye", "J.D. Salinger", "Fiction", 1951, 1],
    [6, "Brave New World", "Aldous Huxley", "Dystopian", 1932, 4],
]
columns = [
    "book_id",
    "title",
    "author",
    "genre",
    "publication_year",
    "total_copies",
]
library_books = spark.createDataFrame(data=data, schema=columns)


data = [
    [1, 1, "Alice Smith", "2024-01-15", None],
    [2, 1, "Bob Johnson", "2024-01-20", None],
    [3, 2, "Carol White", "2024-01-10", "2024-01-25"],
    [4, 3, "David Brown", "2024-02-01", None],
    [5, 4, "Emma Wilson", "2024-01-05", None],
    [6, 5, "Frank Davis", "2024-01-18", "2024-02-10"],
    [7, 1, "Grace Miller", "2024-02-05", None],
    [8, 6, "Henry Taylor", "2024-01-12", None],
    [9, 2, "Ivan Clark", "2024-02-12", None],
    [10, 2, "Jane Adams", "2024-02-15", None],
]
columns = [
    "record_id",
    "book_id",
    "borrower_name",
    "borrow_date",
    "return_date",
]
borrowing_records = spark.createDataFrame(data=data, schema=columns)

borrowers = (
    borrowing_records.filter(borrowing_records.return_date.isNull())
    .groupBy("book_id")
    .count()
    .withColumnRenamed("count", "current_borrowers")
)

df = library_books.join(borrowers, on="book_id", how="inner")

df = df.withColumn("borrowing", df.total_copies - df.current_borrowers)

df = df.filter(df.borrowing == 0).select(
    "book_id",
    "title",
    "author",
    "genre",
    "publication_year",
    "current_borrowers",
)

df.show()
