import pandas as pd

data = [
    ["A", "Math"],
    ["B", "English"],
    ["C", "Math"],
    ["D", "Biology"],
    ["E", "Math"],
    ["F", "Computer"],
    ["G", "Math"],
    ["H", "Math"],
    ["I", "Math"],
]
courses = pd.DataFrame(data, columns=["student", "class"]).astype(
    {"student": "object", "class": "object"}
)


def find_classes(courses: pd.DataFrame) -> pd.DataFrame:
    df = courses["class"].value_counts().reset_index()

    return df[["class"]][df["count"] >= 5]


df = find_classes(courses)
print(df.head())
