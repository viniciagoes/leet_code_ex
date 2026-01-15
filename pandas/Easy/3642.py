import pandas as pd

data = [
    [1, "The Great Gatsby", "F. Scott", "Fiction", 180],
    [2, "To Kill a Mockingbird", "Harper Lee", "Fiction", 281],
    [3, "1984", "George Orwell", "Dystopian", 328],
    [4, "Pride and Prejudice", "Jane Austen", "Romance", 432],
    [5, "The Catcher in the Rye", "J.D. Salinger", "Fiction", 277],
]
books = pd.DataFrame(
    data,
    columns={
        "book_id": pd.Series(dtype="int64"),  # SERIAL -> int64
        "title": pd.Series(dtype="string"),  # VARCHAR -> string dtype
        "author": pd.Series(dtype="string"),  # VARCHAR -> string dtype
        "genre": pd.Series(dtype="string"),  # VARCHAR -> string dtype
        "pages": pd.Series(dtype="int64"),  # INTEGER -> int64
    },
)
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
reading_sessions = pd.DataFrame(
    data,
    columns={
        "session_id": pd.Series(dtype="int64"),  # NUMBER -> int64
        "book_id": pd.Series(dtype="int64"),  # NUMBER -> int64
        "reader_name": pd.Series(dtype="string"),  # VARCHAR2 -> string dtype
        "pages_read": pd.Series(dtype="int64"),  # NUMBER -> int64
        "session_rating": pd.Series(dtype="int64"),  # NUMBER -> int64
    },
)


def find_polarized_books(
    books: pd.DataFrame, reading_sessions: pd.DataFrame
) -> pd.DataFrame:
    # Fix for leetcode incorrect rounding in pandas
    round2 = lambda x: round(x + 0.00001, 2)

    # Determine the necessary counts and max/min per book_id
    df = (
        reading_sessions.groupby("book_id")
        .agg(
            three_count=("session_rating", lambda x: (x == 3).sum()),
            total_count=("session_rating", "count"),
            max_score=("session_rating", "max"),
            min_score=("session_rating", "min"),
        )
        .reset_index()
    )

    # Compute the required data per book_id
    df["rating_spread"] = df.max_score - df.min_score
    df["polarization_score"] = round2(
        (df.total_count - df.three_count) / df.total_count
    )

    # Filter as directed
    df = df[
        (df.min_score < 3)
        & (df.max_score > 3)
        & (df.total_count >= 5)
        & (df.polarization_score >= 0.6)
    ]

    # Sort rows and edit columns as directed
    return (
        df.merge(books)
        .sort_values(["polarization_score", "title"], ascending=[0, 0])
        .iloc[:, [0, 7, 8, 9, 10, 5, 6]]
    )


df = find_polarized_books(books, reading_sessions)
df.head()
