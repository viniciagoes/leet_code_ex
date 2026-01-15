import pandas as pd

data = [
    [1, "The Great Gatsby", "F. Scott", "Fiction", 1925, 3],
    [2, "To Kill a Mockingbird", "Harper Lee", "Fiction", 1960, 3],
    [3, "1984", "George Orwell", "Dystopian", 1949, 1],
    [4, "Pride and Prejudice", "Jane Austen", "Romance", 1813, 2],
    [5, "The Catcher in the Rye", "J.D. Salinger", "Fiction", 1951, 1],
    [6, "Brave New World", "Aldous Huxley", "Dystopian", 1932, 4],
]
library_books = pd.DataFrame(
    data,
    columns={
        "book_id": pd.Series(dtype="int"),
        "title": pd.Series(dtype="str"),
        "author": pd.Series(dtype="str"),
        "genre": pd.Series(dtype="str"),
        "publication_year": pd.Series(dtype="int"),
        "total_copies": pd.Series(dtype="int"),
    },
)
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
borrowing_records = pd.DataFrame(
    data,
    columns={
        "record_id": pd.Series(dtype="int"),
        "book_id": pd.Series(dtype="int"),
        "borrower_name": pd.Series(dtype="str"),
        "borrow_date": pd.Series(dtype="datetime64[ns]"),
        "return_date": pd.Series(dtype="datetime64[ns]"),
    },
)


def find_books_with_no_available_copies(
    library_books: pd.DataFrame, borrowing_records: pd.DataFrame
) -> pd.DataFrame:
    borrowers = (
        borrowing_records[borrowing_records["return_date"].isna()]
        .groupby("book_id")["borrower_name"]
        .count()
        .reset_index()
        .rename(columns={"borrower_name": "borrowers"})
    )

    df = library_books.merge(borrowers, on="book_id", how="inner")[
        [
            "book_id",
            "title",
            "author",
            "genre",
            "publication_year",
            "total_copies",
            "borrowers",
        ]
    ]

    df["current_borrowers"] = df["total_copies"] - df["borrowers"]
    df = df[df["current_borrowers"] == 0][
        ["book_id", "title", "author", "genre", "publication_year", "total_copies"]
    ]
    df.rename(columns={"total_copies": "current_borrowers"}, inplace=True)
    df.sort_values(
        ["current_borrowers", "title"], ascending=[False, True], inplace=True
    )

    return df


df = find_books_with_no_available_copies(library_books, borrowing_records)
df.head()
