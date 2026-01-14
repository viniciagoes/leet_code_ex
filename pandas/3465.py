import pandas as pd

data = [
    [1, "Widget A", "This is a sample product with SN1234-5678"],
    [2, "Widget B", "A product with serial SN9876-1234 in the description"],
    [3, "Widget C", "Product SN1234-56789 is available now"],
    [4, "Widget D", "No serial number here"],
    [5, "Widget E", "Check out SN4321-8765 in this description"],
]
products = pd.DataFrame(
    data, columns=["product_id", "product_name", "description"]
).astype({"product_id": "int32", "product_name": "string", "description": "string"})
products.head()


def find_valid_serial_products(products: pd.DataFrame) -> pd.DataFrame:
    df = products[products["description"].str.contains(r"\bSN[0-9]{4}-[0-9]{4}\b")]

    return df.sort_values("product_id")


df = find_valid_serial_products(products)
df.head()
