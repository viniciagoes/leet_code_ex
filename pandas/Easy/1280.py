import pandas as pd

data = [[1, "Alice"], [2, "Bob"], [13, "John"], [6, "Alex"]]
students = pd.DataFrame(data, columns=["student_id", "student_name"]).astype(
    {"student_id": "Int64", "student_name": "object"}
)
data = [["Math"], ["Physics"], ["Programming"]]
subjects = pd.DataFrame(data, columns=["subject_name"]).astype(
    {"subject_name": "object"}
)
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
examinations = pd.DataFrame(data, columns=["student_id", "subject_name"]).astype(
    {"student_id": "Int64", "subject_name": "object"}
)


def students_and_examinations(
    students: pd.DataFrame, subjects: pd.DataFrame, examinations: pd.DataFrame
) -> pd.DataFrame:
    stu_vs_sub = students.merge(subjects, how="cross")
    stu_vs_sub.sort_values(by="student_id", inplace=True)

    df = stu_vs_sub.merge(
        examinations, on=["student_id", "subject_name"], how="left", indicator=True
    )

    def merge_map(merge_type):
        if merge_type == "both":
            return 1
        else:
            return pd.NA

    df["attended_exams"] = df["_merge"].map(merge_map)
    df = (
        df.groupby(["student_id", "student_name", "subject_name"], dropna=False)[
            "attended_exams"
        ]
        .count()
        .reset_index()
    )

    return df


df = students_and_examinations(students, subjects, examinations)
print(df.head(30))
