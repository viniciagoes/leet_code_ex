from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

data = [
    [1, "John", 100000, 6, "4/1/2006"],
    [2, "Amy", 12000, 5, "5/1/2010"],
    [3, "Mark", 65000, 12, "12/25/2008"],
    [4, "Pam", 25000, 25, "1/1/2005"],
    [5, "Alex", 5000, 10, "2/3/2007"],
]
sales_person = spark.createDataFrame(
    data, ["sales_id", "name", "salary", "commission_rate", "hire_date"]
)
data = [
    [1, "RED", "Boston"],
    [2, "ORANGE", "New York"],
    [3, "YELLOW", "Boston"],
    [4, "GREEN", "Austin"],
]
company = spark.createDataFrame(data, ["com_id", "name", "city"])
data = [
    [1, "1/1/2014", 3, 4, 10000],
    [2, "2/1/2014", 4, 5, 5000],
    [3, "3/1/2014", 1, 1, 50000],
    [4, "4/1/2014", 1, 4, 25000],
]
orders = spark.createDataFrame(
    data, ["order_id", "order_date", "com_id", "sales_id", "amount"]
)

# Get id from company I do not want
red_id = company.filter(company.name == "RED").select("com_id").collect()[0][0]

# Get all sales_id related to this company and transform to list
red_salesp_id = orders.filter(orders.com_id == 1).select("sales_id").distinct()
red_salesp_id = [a[0] for a in red_salesp_id.collect()]

# Get all sales person not in previous list
df = sales_person.filter(~(sales_person.sales_id.isin(red_salesp_id))).select("name")
df.show()
