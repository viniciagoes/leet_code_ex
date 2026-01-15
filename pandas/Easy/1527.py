import pandas as pd

data = [
    [1, "Daniel", "YFEV COUGH"],
    [2, "Alice", ""],
    [3, "Bob", "DIAB100 MYOP"],
    [4, "George", "ACNE DIAB100"],
    [5, "Alain", "DIAB201"],
]
patients = pd.DataFrame(
    data, columns=["patient_id", "patient_name", "conditions"]
).astype({"patient_id": "int64", "patient_name": "object", "conditions": "object"})


def find_patients(patients: pd.DataFrame) -> pd.DataFrame:
    df = patients[
        patients["conditions"].str.contains(" DIAB1")
        | patients["conditions"].str.startswith("DIAB1")
    ]
    return df


df = find_patients(patients)
